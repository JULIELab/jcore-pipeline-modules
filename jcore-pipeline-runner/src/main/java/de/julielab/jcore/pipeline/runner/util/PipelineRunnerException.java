package de.julielab.jcore.pipeline.runner.util;

public class PipelineRunnerException extends Exception {
    public PipelineRunnerException() {
    }

    public PipelineRunnerException(String message) {
        super(message);
    }

    public PipelineRunnerException(String message, Throwable cause) {
        super(message, cause);
    }

    public PipelineRunnerException(Throwable cause) {
        super(cause);
    }

    public PipelineRunnerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
