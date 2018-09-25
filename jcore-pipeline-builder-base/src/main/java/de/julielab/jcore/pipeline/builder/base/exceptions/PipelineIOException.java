package de.julielab.jcore.pipeline.builder.base.exceptions;

public class PipelineIOException extends Exception {

    public PipelineIOException() {
    }

    public PipelineIOException(String message) {
        super(message);
    }

    public PipelineIOException(String message, Throwable cause) {
        super(message, cause);
    }

    public PipelineIOException(Throwable cause) {
        super(cause);
    }

    public PipelineIOException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
