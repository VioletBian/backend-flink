package com.dataprocessor.flink.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebCorsConfig implements WebMvcConfigurer {

    private final BackendFlinkProperties backendFlinkProperties;

    public WebCorsConfig(BackendFlinkProperties backendFlinkProperties) {
        this.backendFlinkProperties = backendFlinkProperties;
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        // 中文说明：开发态前后端分端口联调时，统一放开 /dp/** 的跨域；origin 仍通过配置显式收口，不直接用 *。
        registry.addMapping("/dp/**")
            .allowedOriginPatterns(backendFlinkProperties.getCors().getAllowedOriginPatterns().toArray(String[]::new))
            .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS")
            .allowedHeaders("*")
            .allowCredentials(true)
            .maxAge(3600);
    }
}
