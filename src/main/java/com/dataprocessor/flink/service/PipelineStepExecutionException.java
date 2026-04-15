package com.dataprocessor.flink.service;

import com.dataprocessor.flink.planner.OperationSpec;

// 中文说明：统一把运行期的算子失败包装成 step 级异常，确保前端能拿到“第几个 step、是什么算子、为什么失败”。
public class PipelineStepExecutionException extends IllegalArgumentException {

    public PipelineStepExecutionException(OperationSpec spec, String detail) {
        this(spec.getSourceStepIndex(), spec.getType(), detail, null);
    }

    public PipelineStepExecutionException(OperationSpec spec, String detail, Throwable cause) {
        this(spec.getSourceStepIndex(), spec.getType(), detail, cause);
    }

    public PipelineStepExecutionException(int sourceStepIndex, String operatorType, String detail) {
        this(sourceStepIndex, operatorType, detail, null);
    }

    public PipelineStepExecutionException(int sourceStepIndex, String operatorType, String detail, Throwable cause) {
        super(buildMessage(sourceStepIndex, operatorType, detail), cause);
    }

    public static PipelineStepExecutionException wrap(OperationSpec spec, RuntimeException cause) {
        if (cause instanceof PipelineStepExecutionException stepExecutionException) {
            return stepExecutionException;
        }
        String detail = cause.getMessage();
        if (detail == null || detail.isBlank()) {
            detail = cause.getClass().getSimpleName();
        }
        return new PipelineStepExecutionException(spec, detail, cause);
    }

    private static String buildMessage(int sourceStepIndex, String operatorType, String detail) {
        String normalizedDetail = detail == null || detail.isBlank() ? "Unknown execution error." : detail;
        return "Step " + (sourceStepIndex + 1) + " (" + operatorType + ") failed: " + normalizedDetail;
    }
}
