package de.julielab.jcore.pipeline.builder.base.exceptions;

public class MavenException extends Throwable {
    public MavenException() {
    }

    public MavenException(String message) {
        super(message);
    }

    public MavenException(String message, Throwable cause) {
        super(message, cause);
    }

    public MavenException(Throwable cause) {
        super(cause);
    }

    public MavenException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
