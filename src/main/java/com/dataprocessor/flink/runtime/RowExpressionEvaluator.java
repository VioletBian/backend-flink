package com.dataprocessor.flink.runtime;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.ParseException;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;

// 中文说明：这里把 filter/tag 这类“轻表达式”统一收口成一个共享求值器，避免 planner 和 executor 各维护一套规则。
@Component
public class RowExpressionEvaluator {

    private static final String DEFAULT_CONDITION = "index > -1";
    private static final ExpressionParser PARSER = new SpelExpressionParser();
    private static final Set<String> RESERVED_WORDS = Set.of(
        "and",
        "or",
        "not",
        "true",
        "false",
        "null",
        "True",
        "False",
        "None",
        "index"
    );
    private static final List<String> UNSUPPORTED_PYTHON_FRAGMENTS = List.of(
        ".str.",
        ".dt.",
        ".isin(",
        ".shift(",
        ".rolling(",
        ".diff(",
        "[",
        "]"
    );

    public boolean supportsBooleanExpression(String expression, List<String> availableColumns) {
        try {
            compileBooleanExpression(expression, availableColumns);
            return true;
        } catch (IllegalArgumentException exception) {
            return false;
        }
    }

    public CompiledBooleanExpression compileBooleanExpression(String expression, List<String> availableColumns) {
        if (expression == null || expression.isBlank() || DEFAULT_CONDITION.equals(expression.trim())) {
            return CompiledBooleanExpression.ofAlwaysTrue();
        }
        ExpressionBinding binding = compileInternal(expression, availableColumns);
        return new CompiledBooleanExpression(binding.spelExpression(), binding.variableMapping(), false);
    }

    public boolean supportsValueExpression(String expression, List<String> availableColumns) {
        try {
            compileValueExpression(expression, availableColumns);
            return true;
        } catch (IllegalArgumentException exception) {
            return false;
        }
    }

    public CompiledValueExpression compileValueExpression(String expression, List<String> availableColumns) {
        if (expression == null || expression.isBlank()) {
            throw new IllegalArgumentException("Vectorized value expression cannot be empty.");
        }
        ExpressionBinding binding = compileInternal(expression, availableColumns);
        return new CompiledValueExpression(binding.spelExpression(), binding.variableMapping());
    }

    public List<String> extractReferencedColumns(String expression, List<String> availableColumns) {
        if (expression == null || expression.isBlank() || DEFAULT_CONDITION.equals(expression.trim())) {
            return List.of();
        }
        ExpressionBinding binding = compileInternal(expression, availableColumns);
        return binding.variableMapping().values().stream()
            .filter(column -> !"__index__".equals(column))
            .toList();
    }

    public boolean evaluateBoolean(RuntimeRow row, CompiledBooleanExpression expression) {
        return evaluateBooleanExpression(row, expression);
    }

    public Object evaluateValue(RuntimeRow row, CompiledValueExpression expression) {
        return evaluateValueExpression(row, expression);
    }

    public static boolean evaluateBooleanExpression(RuntimeRow row, CompiledBooleanExpression expression) {
        if (expression.alwaysTrue()) {
            return true;
        }
        Expression spelExpression = PARSER.parseExpression(expression.spelExpression());
        StandardEvaluationContext context = new StandardEvaluationContext();
        for (Map.Entry<String, String> entry : expression.variableMapping().entrySet()) {
            if ("__index__".equals(entry.getValue())) {
                context.setVariable(entry.getKey(), row.getRowId());
                continue;
            }
            context.setVariable(entry.getKey(), row.getValues().get(entry.getValue()));
        }
        Boolean result = spelExpression.getValue(context, Boolean.class);
        return Boolean.TRUE.equals(result);
    }

    public static Object evaluateValueExpression(RuntimeRow row, CompiledValueExpression expression) {
        Expression spelExpression = PARSER.parseExpression(expression.spelExpression());
        StandardEvaluationContext context = new StandardEvaluationContext();
        for (Map.Entry<String, String> entry : expression.variableMapping().entrySet()) {
            if ("__index__".equals(entry.getValue())) {
                context.setVariable(entry.getKey(), row.getRowId());
                continue;
            }
            context.setVariable(entry.getKey(), row.getValues().get(entry.getValue()));
        }
        return spelExpression.getValue(context);
    }

    private ExpressionBinding compileInternal(String expression, List<String> availableColumns) {
        for (String unsupportedFragment : UNSUPPORTED_PYTHON_FRAGMENTS) {
            if (expression.contains(unsupportedFragment)) {
                throw new IllegalArgumentException("Unsupported python-style condition fragment: " + unsupportedFragment);
            }
        }
        ExpressionBinding binding = rewriteToSpel(expression, availableColumns);
        try {
            PARSER.parseExpression(binding.spelExpression());
            return binding;
        } catch (ParseException exception) {
            throw new IllegalArgumentException("Unsupported expression: " + expression, exception);
        }
    }

    private ExpressionBinding rewriteToSpel(String expression, List<String> availableColumns) {
        String normalizedExpression = normalizeLogicalOperators(expression)
            .replace("&&", " and ")
            .replace("||", " or ")
            .replace(" None", " null")
            .replace("(None", "(null")
            .replace(": None", ": null")
            .replace(" True", " true")
            .replace(" False", " false");

        StringBuilder rewritten = new StringBuilder();
        LinkedHashMap<String, String> variableMapping = new LinkedHashMap<>();
        Map<String, String> columnToVariable = new LinkedHashMap<>();
        Set<String> availableColumnSet = new LinkedHashSet<>(availableColumns);

        int cursor = 0;
        while (cursor < normalizedExpression.length()) {
            char currentChar = normalizedExpression.charAt(cursor);
            if (currentChar == '`') {
                int end = normalizedExpression.indexOf('`', cursor + 1);
                if (end < 0) {
                    throw new IllegalArgumentException("Unclosed backtick column reference in expression.");
                }
                String columnName = normalizedExpression.substring(cursor + 1, end);
                if (!availableColumnSet.contains(columnName)) {
                    throw new IllegalArgumentException("Unknown column in expression: " + columnName);
                }
                rewritten.append('#').append(resolveVariable(columnName, columnToVariable, variableMapping));
                cursor = end + 1;
                continue;
            }
            if (currentChar == '\'' || currentChar == '"') {
                int end = consumeQuotedLiteral(normalizedExpression, cursor, currentChar);
                rewritten.append(normalizedExpression, cursor, end);
                cursor = end;
                continue;
            }
            if (Character.isLetter(currentChar) || currentChar == '_') {
                int end = cursor + 1;
                while (end < normalizedExpression.length()) {
                    char nextChar = normalizedExpression.charAt(end);
                    if (!Character.isLetterOrDigit(nextChar) && nextChar != '_') {
                        break;
                    }
                    end++;
                }
                String token = normalizedExpression.substring(cursor, end);
                if (RESERVED_WORDS.contains(token)) {
                    if ("index".equals(token)) {
                        rewritten.append("#").append(resolveVariable("__index__", columnToVariable, variableMapping));
                    } else if ("null".equals(token) || "None".equals(token)) {
                        rewritten.append("null");
                    } else if ("true".equals(token) || "false".equals(token) || "True".equals(token) || "False".equals(token)) {
                        rewritten.append(token.toLowerCase());
                    } else {
                        rewritten.append(token);
                    }
                } else if (availableColumnSet.contains(token)) {
                    rewritten.append('#').append(resolveVariable(token, columnToVariable, variableMapping));
                } else {
                    throw new IllegalArgumentException("Unsupported token in expression: " + token);
                }
                cursor = end;
                continue;
            }
            rewritten.append(currentChar);
            cursor++;
        }
        return new ExpressionBinding(rewritten.toString(), variableMapping);
    }

    // 中文说明：前端轻表达式经常把布尔连接符写成单个 `&` / `|`，
    // 这里在不进入字符串字面量的前提下统一转成 SpEL 可识别的 `and` / `or`。
    private String normalizeLogicalOperators(String expression) {
        StringBuilder normalized = new StringBuilder();
        int cursor = 0;
        while (cursor < expression.length()) {
            char current = expression.charAt(cursor);
            if (current == '\'' || current == '"') {
                int end = consumeQuotedLiteral(expression, cursor, current);
                normalized.append(expression, cursor, end);
                cursor = end;
                continue;
            }
            if (current == '&') {
                if (cursor + 1 < expression.length() && expression.charAt(cursor + 1) == '&') {
                    cursor += 2;
                } else {
                    cursor += 1;
                }
                normalized.append(" and ");
                continue;
            }
            if (current == '|') {
                if (cursor + 1 < expression.length() && expression.charAt(cursor + 1) == '|') {
                    cursor += 2;
                } else {
                    cursor += 1;
                }
                normalized.append(" or ");
                continue;
            }
            normalized.append(current);
            cursor++;
        }
        return normalized.toString();
    }

    private int consumeQuotedLiteral(String expression, int start, char quote) {
        int cursor = start + 1;
        while (cursor < expression.length()) {
            char current = expression.charAt(cursor);
            if (current == '\\') {
                cursor += 2;
                continue;
            }
            if (current == quote) {
                return cursor + 1;
            }
            cursor++;
        }
        throw new IllegalArgumentException("Unclosed string literal in expression.");
    }

    private String resolveVariable(
        String source,
        Map<String, String> columnToVariable,
        Map<String, String> variableMapping
    ) {
        String existing = columnToVariable.get(source);
        if (existing != null) {
            return existing;
        }
        String variableName = "v" + columnToVariable.size();
        columnToVariable.put(source, variableName);
        variableMapping.put(variableName, source);
        return variableName;
    }

    private record ExpressionBinding(String spelExpression, LinkedHashMap<String, String> variableMapping) {
    }

    public record CompiledBooleanExpression(
        String spelExpression,
        LinkedHashMap<String, String> variableMapping,
        boolean alwaysTrue
    ) implements Serializable {
        public static CompiledBooleanExpression ofAlwaysTrue() {
            return new CompiledBooleanExpression("true", new LinkedHashMap<>(), true);
        }
    }

    public record CompiledValueExpression(
        String spelExpression,
        LinkedHashMap<String, String> variableMapping
    ) implements Serializable {
    }
}
