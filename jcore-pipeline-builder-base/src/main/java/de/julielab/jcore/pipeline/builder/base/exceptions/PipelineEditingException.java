package de.julielab.jcore.pipeline.builder.base.exceptions;

public class PipelineEditingException extends Exception {
    public PipelineEditingException() {
    }

    public PipelineEditingException(String message) {
        super(message);
    }

    public PipelineEditingException(String message, Throwable cause) {
        super(message, cause);
    }

    public PipelineEditingException(Throwable cause) {
        super(cause);
    }

    public PipelineEditingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
