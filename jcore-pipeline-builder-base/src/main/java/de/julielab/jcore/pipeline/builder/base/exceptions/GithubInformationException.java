package de.julielab.jcore.pipeline.builder.base.exceptions;

public class GithubInformationException extends Exception {
    public GithubInformationException() {
    }

    public GithubInformationException(String message) {
        super(message);
    }

    public GithubInformationException(String message, Throwable cause) {
        super(message, cause);
    }

    public GithubInformationException(Throwable cause) {
        super(cause);
    }

    public GithubInformationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
