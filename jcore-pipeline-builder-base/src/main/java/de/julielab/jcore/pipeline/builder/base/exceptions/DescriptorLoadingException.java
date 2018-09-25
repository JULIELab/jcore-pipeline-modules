package de.julielab.jcore.pipeline.builder.base.exceptions;

public class DescriptorLoadingException extends Exception {
    public DescriptorLoadingException() {
    }

    public DescriptorLoadingException(String message) {
        super(message);
    }

    public DescriptorLoadingException(String message, Throwable cause) {
        super(message, cause);
    }

    public DescriptorLoadingException(Throwable cause) {
        super(cause);
    }

    public DescriptorLoadingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
