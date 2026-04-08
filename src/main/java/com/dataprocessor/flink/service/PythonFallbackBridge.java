package com.dataprocessor.flink.service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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

            List<String> command = new ArrayList<>();
            command.add(properties.getPythonCommand());
            command.add(resolveRunnerPath().toString());
            command.add("--input-file");
            command.add(inputFile.toString());
            command.add("--pipeline-file");
            command.add(pipelineFile.toString());
            command.add("--output-file");
            command.add(outputFile.toString());
            if (debug) {
                command.add("--debug");
            }

            ProcessBuilder processBuilder = new ProcessBuilder(command);
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

    private Path resolveRunnerPath() {
        Path workspaceRoot = resolveWorkspaceRoot();
        Path configuredRunnerPath = Path.of(properties.getPythonRunnerPath());
        if (configuredRunnerPath.isAbsolute()) {
            return configuredRunnerPath.normalize();
        }

        Path candidate = workspaceRoot.resolve(configuredRunnerPath).normalize();
        if (Files.exists(candidate)) {
            return candidate;
        }

        return resolveProcessWorkingDirectory().resolve(configuredRunnerPath).normalize();
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
}
