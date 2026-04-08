package com.dataprocessor.flink.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "backend.flink")
public class BackendFlinkProperties {

    private String mongoCollectionName = "pipelines";
    private String pythonCommand = "python3";
    private String pythonRunnerPath = "backend/scripts/reference_runner_cli.py";
    private String pythonWorkingDirectory = ".";

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
}
