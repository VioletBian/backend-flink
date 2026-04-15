package com.dataprocessor.flink.controller;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.multipart.MaxUploadSizeExceededException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, Object> handleIllegalArgument(IllegalArgumentException exception) {
        // 中文说明：参数/步骤类错误通常属于可预期失败，保留 warning 级别并带上堆栈，便于定位具体 step。
        log.warn("Request failed with invalid input or step execution error.", exception);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("detail", exception.getMessage());
        return payload;
    }

    @ExceptionHandler(MaxUploadSizeExceededException.class)
    @ResponseStatus(HttpStatus.PAYLOAD_TOO_LARGE)
    public Map<String, Object> handleMaxUploadSizeExceeded(MaxUploadSizeExceededException exception) {
        log.warn("Request failed because uploaded file exceeded configured multipart limits.", exception);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("detail", "Uploaded file exceeds the configured 100MB multipart limit.");
        return payload;
    }

    @ExceptionHandler(IllegalStateException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Map<String, Object> handleIllegalState(IllegalStateException exception) {
        log.error("Request failed with internal server error.", exception);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("detail", exception.getMessage());
        return payload;
    }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Map<String, Object> handleUnexpectedException(Exception exception) {
        // 中文说明：兜底异常统一完整打印，避免连接被断开时控制台里只剩“Started”而没有任何失败线索。
        log.error("Request failed with unexpected exception.", exception);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("detail", exception.getMessage());
        return payload;
    }
}
