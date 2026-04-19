package com.dataprocessor.flink.service;

import java.nio.file.Path;
import java.util.List;

import com.dataprocessor.flink.config.BackendFlinkProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PythonFallbackBridgeTest {

    @Test
    void buildsDirectPythonCommandWhenActivationScriptIsNotConfigured() {
        BackendFlinkProperties properties = new BackendFlinkProperties();
        properties.setPythonCommand("python3");
        properties.setPythonRunnerPath("backend/scripts/reference_runner_cli.py");
        properties.setPythonWorkingDirectory(".");

        PythonFallbackBridge bridge = new PythonFallbackBridge(properties, new ObjectMapper());

        PythonFallbackBridge.PythonProcessSpec processSpec = bridge.buildProcessSpec(
            Path.of("/tmp/input.csv"),
            Path.of("/tmp/pipeline.json"),
            Path.of("/tmp/output.json"),
            true,
            "Linux"
        );

        Assertions.assertEquals("python3", processSpec.command().get(0));
        Assertions.assertEquals("--debug", processSpec.command().get(processSpec.command().size() - 1));
        Assertions.assertEquals(
            List.of(
                "python3",
                Path.of("").toAbsolutePath().normalize().getParent().resolve("backend/scripts/reference_runner_cli.py").toString(),
                "--input-file",
                "/tmp/input.csv",
                "--pipeline-file",
                "/tmp/pipeline.json",
                "--output-file",
                "/tmp/output.json",
                "--debug"
            ),
            processSpec.command()
        );
    }

    @Test
    void buildsWindowsActivationCommandWhenActivationScriptIsConfigured() {
        BackendFlinkProperties properties = new BackendFlinkProperties();
        properties.setPythonCommand("python");
        properties.setPythonActivationScript("venv312/Scripts/activate.bat");
        properties.setPythonRunnerPath("backend/scripts/reference_runner_cli.py");
        properties.setPythonWorkingDirectory(".");

        PythonFallbackBridge bridge = new PythonFallbackBridge(properties, new ObjectMapper());

        PythonFallbackBridge.PythonProcessSpec processSpec = bridge.buildProcessSpec(
            Path.of("C:/temp/input.csv"),
            Path.of("C:/temp/pipeline.json"),
            Path.of("C:/temp/output.json"),
            false,
            "Windows 11"
        );

        Assertions.assertEquals(List.of("cmd.exe", "/c"), processSpec.command().subList(0, 2));
        Assertions.assertTrue(processSpec.command().get(2).contains("call "));
        Assertions.assertTrue(processSpec.command().get(2).contains("activate.bat"));
        Assertions.assertTrue(processSpec.command().get(2).contains("\"python\""));
        Assertions.assertTrue(processSpec.command().get(2).contains("\"--output-file\""));
    }
}
