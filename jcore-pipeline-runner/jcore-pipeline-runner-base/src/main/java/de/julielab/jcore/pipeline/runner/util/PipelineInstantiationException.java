package de.julielab.jcore.pipeline.runner.util;

public class PipelineInstantiationException extends PipelineRunnerException {
    public PipelineInstantiationException() {
    }

    public PipelineInstantiationException(String message) {
        super(message);
    }

    public PipelineInstantiationException(String message, Throwable cause) {
        super(message, cause);
    }

    public PipelineInstantiationException(Throwable cause) {
        super(cause);
    }

    public PipelineInstantiationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
