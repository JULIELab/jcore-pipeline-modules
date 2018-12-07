package de.julielab.jcore.pipeline.runner.util;

public class PipelineRunningException extends PipelineRunnerException {
    public PipelineRunningException() {
    }

    public PipelineRunningException(String message) {
        super(message);
    }

    public PipelineRunningException(String message, Throwable cause) {
        super(message, cause);
    }

    public PipelineRunningException(Throwable cause) {
        super(cause);
    }

    public PipelineRunningException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
