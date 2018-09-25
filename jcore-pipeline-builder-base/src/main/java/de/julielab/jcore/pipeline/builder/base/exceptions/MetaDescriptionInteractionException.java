package de.julielab.jcore.pipeline.builder.base.exceptions;

/**
 * The {@link de.julielab.jcore.pipeline.builder.base.main.MetaDescription} class contains IO operations at places
 * where one would not really expect them, e.g. getter and even setter. The reason is lazy loading, we don't
 * want to load all artifacts for all meta descriptions at application startup because it takes very long and most
 * artifacts won't be used anyway.
 */
public class MetaDescriptionInteractionException extends RuntimeException {
    public MetaDescriptionInteractionException() {
    }

    public MetaDescriptionInteractionException(String message) {
        super(message);
    }

    public MetaDescriptionInteractionException(String message, Throwable cause) {
        super(message, cause);
    }

    public MetaDescriptionInteractionException(Throwable cause) {
        super(cause);
    }

    public MetaDescriptionInteractionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
