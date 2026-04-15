package com.dataprocessor.flink.service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.dataprocessor.flink.config.BackendFlinkProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;

@Service
public class PythonFallbackBridge {

    private final BackendFlinkProperties properties;
    private final ObjectMapper objectMapper;

    public PythonFallbackBridge(BackendFlinkProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    public Map<String, Object> run(byte[] fileBytes, String canonicalPipelineJson, boolean debug) {
        Path inputFile = null;
        Path pipelineFile = null;
        Path outputFile = null;

        try {
            inputFile = Files.createTempFile("backend-flink-input-", ".csv");
            pipelineFile = Files.createTempFile("backend-flink-pipeline-", ".json");
            outputFile = Files.createTempFile("backend-flink-output-", ".json");

            Files.write(inputFile, fileBytes);
            Files.writeString(pipelineFile, canonicalPipelineJson, StandardCharsets.UTF_8);

            PythonProcessSpec processSpec = buildProcessSpec(inputFile, pipelineFile, outputFile, debug);
            ProcessBuilder processBuilder = new ProcessBuilder(processSpec.command());
            processBuilder.redirectErrorStream(true);
            processBuilder.directory(resolveProcessWorkingDirectory().toFile());

            Process process = processBuilder.start();
            String stdout = StreamUtils.copyToString(process.getInputStream(), StandardCharsets.UTF_8);
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IllegalStateException("Python fallback bridge failed: " + stdout.trim());
            }

            String outputJson = Files.readString(outputFile, StandardCharsets.UTF_8);
            return objectMapper.readValue(outputJson, new TypeReference<LinkedHashMap<String, Object>>() {
            });
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to execute Python fallback bridge.", exception);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Python fallback bridge was interrupted.", exception);
        } finally {
            deleteIfExists(inputFile);
            deleteIfExists(pipelineFile);
            deleteIfExists(outputFile);
        }
    }

    // 中文说明：公司远端环境需要先激活虚拟环境时，统一在这里组装 shell 命令；未配置激活脚本时继续保持原来的直接执行方式。
    PythonProcessSpec buildProcessSpec(Path inputFile, Path pipelineFile, Path outputFile, boolean debug) {
        return buildProcessSpec(inputFile, pipelineFile, outputFile, debug, System.getProperty("os.name"));
    }

    PythonProcessSpec buildProcessSpec(
        Path inputFile,
        Path pipelineFile,
        Path outputFile,
        boolean debug,
        String osName
    ) {
        List<String> pythonArguments = new ArrayList<>();
        pythonArguments.add(resolveRunnerPath().toString());
        pythonArguments.add("--input-file");
        pythonArguments.add(inputFile.toString());
        pythonArguments.add("--pipeline-file");
        pythonArguments.add(pipelineFile.toString());
        pythonArguments.add("--output-file");
        pythonArguments.add(outputFile.toString());
        if (debug) {
            pythonArguments.add("--debug");
        }

        String activationScript = properties.getPythonActivationScript();
        if (activationScript == null || activationScript.isBlank()) {
            List<String> directCommand = new ArrayList<>();
            directCommand.add(properties.getPythonCommand());
            directCommand.addAll(pythonArguments);
            return new PythonProcessSpec(directCommand);
        }

        Path activationScriptPath = resolveActivationScriptPath();
        if (isWindowsShell(osName) || isWindowsBatchScript(activationScriptPath)) {
            return new PythonProcessSpec(
                List.of("cmd.exe", "/c", buildWindowsShellCommand(activationScriptPath, pythonArguments))
            );
        }

        return new PythonProcessSpec(
            List.of("/bin/sh", "-lc", buildPosixShellCommand(activationScriptPath, pythonArguments))
        );
    }

    private void deleteIfExists(Path path) {
        if (path == null) {
            return;
        }
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
            // 临时文件清理失败不影响主流程结果。
        }
    }

    private Path resolveProcessWorkingDirectory() {
        Path workspaceRoot = resolveWorkspaceRoot();
        String configuredWorkingDirectory = properties.getPythonWorkingDirectory();
        if (configuredWorkingDirectory == null || configuredWorkingDirectory.isBlank()) {
            return workspaceRoot;
        }

        Path configuredPath = Path.of(configuredWorkingDirectory);
        if (configuredPath.isAbsolute()) {
            return configuredPath.normalize();
        }

        return workspaceRoot.resolve(configuredPath).normalize();
    }

    // 中文说明：runner/activate 这类路径都允许配置成绝对路径，或者相对项目根目录/工作目录的相对路径，方便在本地和远端环境复用。
    private Path resolveActivationScriptPath() {
        String configuredActivationScript = properties.getPythonActivationScript();
        if (configuredActivationScript == null || configuredActivationScript.isBlank()) {
            throw new IllegalStateException("Python activation script is not configured.");
        }
        return resolveConfiguredPath(configuredActivationScript);
    }

    private Path resolveRunnerPath() {
        return resolveConfiguredPath(properties.getPythonRunnerPath());
    }

    private Path resolveConfiguredPath(String configuredPathValue) {
        Path workspaceRoot = resolveWorkspaceRoot();
        Path configuredPath = Path.of(configuredPathValue);
        if (configuredPath.isAbsolute()) {
            return configuredPath.normalize();
        }

        Path workspaceCandidate = workspaceRoot.resolve(configuredPath).normalize();
        if (Files.exists(workspaceCandidate)) {
            return workspaceCandidate;
        }

        return resolveProcessWorkingDirectory().resolve(configuredPath).normalize();
    }

    private Path resolveWorkspaceRoot() {
        Path currentWorkingDirectory = Path.of("").toAbsolutePath().normalize();
        if (Files.exists(currentWorkingDirectory.resolve("backend/scripts/reference_runner_cli.py"))) {
            return currentWorkingDirectory;
        }

        Path parent = currentWorkingDirectory.getParent();
        if (parent != null && Files.exists(parent.resolve("backend/scripts/reference_runner_cli.py"))) {
            return parent;
        }

        return currentWorkingDirectory;
    }

    private boolean isWindowsShell(String osName) {
        return osName != null && osName.toLowerCase(Locale.ROOT).contains("win");
    }

    private boolean isWindowsBatchScript(Path activationScriptPath) {
        String fileName = activationScriptPath.getFileName() == null ? "" : activationScriptPath.getFileName().toString().toLowerCase(Locale.ROOT);
        return fileName.endsWith(".bat") || fileName.endsWith(".cmd");
    }

    private String buildWindowsShellCommand(Path activationScriptPath, List<String> pythonArguments) {
        return "call " + quoteForWindowsCmd(activationScriptPath.toString())
            + " && "
            + joinWindowsCommand(buildPythonCommandTokens(pythonArguments));
    }

    private String buildPosixShellCommand(Path activationScriptPath, List<String> pythonArguments) {
        return ". " + quoteForPosixShell(activationScriptPath.toString())
            + " && exec "
            + joinPosixCommand(buildPythonCommandTokens(pythonArguments));
    }

    private List<String> buildPythonCommandTokens(List<String> pythonArguments) {
        List<String> commandTokens = new ArrayList<>();
        commandTokens.add(properties.getPythonCommand());
        commandTokens.addAll(pythonArguments);
        return commandTokens;
    }

    private String joinWindowsCommand(List<String> commandTokens) {
        return commandTokens.stream()
            .map(this::quoteForWindowsCmd)
            .reduce((left, right) -> left + " " + right)
            .orElse("");
    }

    private String joinPosixCommand(List<String> commandTokens) {
        return commandTokens.stream()
            .map(this::quoteForPosixShell)
            .reduce((left, right) -> left + " " + right)
            .orElse("");
    }

    private String quoteForWindowsCmd(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private String quoteForPosixShell(String value) {
        return "'" + value.replace("'", "'\"'\"'") + "'";
    }

    record PythonProcessSpec(List<String> command) {
    }
}
