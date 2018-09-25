package de.julielab.jcore.pipeline.builder.base.exceptions;

public class PipelineIORuntimeException extends RuntimeException {
    public PipelineIORuntimeException() {
    }

    public PipelineIORuntimeException(String message) {
        super(message);
    }

    public PipelineIORuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public PipelineIORuntimeException(Throwable cause) {
        super(cause);
    }

    public PipelineIORuntimeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
