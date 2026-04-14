package com.dataprocessor.flink.config;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "backend.flink")
public class BackendFlinkProperties {

    private String mongoCollectionName = "pipelines";
    private String pythonCommand = "python3";
    private String pythonRunnerPath = "backend/scripts/reference_runner_cli.py";
    private String pythonWorkingDirectory = ".";
    private CorsProperties cors = new CorsProperties();

    public String getMongoCollectionName() {
        return mongoCollectionName;
    }

    public void setMongoCollectionName(String mongoCollectionName) {
        this.mongoCollectionName = mongoCollectionName;
    }

    public String getPythonCommand() {
        return pythonCommand;
    }

    public void setPythonCommand(String pythonCommand) {
        this.pythonCommand = pythonCommand;
    }

    public String getPythonRunnerPath() {
        return pythonRunnerPath;
    }

    public void setPythonRunnerPath(String pythonRunnerPath) {
        this.pythonRunnerPath = pythonRunnerPath;
    }

    public String getPythonWorkingDirectory() {
        return pythonWorkingDirectory;
    }

    public void setPythonWorkingDirectory(String pythonWorkingDirectory) {
        this.pythonWorkingDirectory = pythonWorkingDirectory;
    }

    public CorsProperties getCors() {
        return cors;
    }

    public void setCors(CorsProperties cors) {
        this.cors = cors;
    }

    public static class CorsProperties {
        private List<String> allowedOriginPatterns = new ArrayList<>(
            List.of(
                "http://localhost:*",
                "http://127.0.0.1:*",
                "https://localhost:*",
                "https://127.0.0.1:*"
            )
        );

        public List<String> getAllowedOriginPatterns() {
            return allowedOriginPatterns;
        }

        public void setAllowedOriginPatterns(List<String> allowedOriginPatterns) {
            this.allowedOriginPatterns = allowedOriginPatterns == null ? new ArrayList<>() : new ArrayList<>(allowedOriginPatterns);
        }
    }
}
