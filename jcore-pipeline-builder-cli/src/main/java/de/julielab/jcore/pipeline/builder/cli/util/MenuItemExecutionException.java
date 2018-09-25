package de.julielab.jcore.pipeline.builder.cli.util;

public class MenuItemExecutionException extends Exception {
    public MenuItemExecutionException() {
    }

    public MenuItemExecutionException(String message) {
        super(message);
    }

    public MenuItemExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public MenuItemExecutionException(Throwable cause) {
        super(cause);
    }

    public MenuItemExecutionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
