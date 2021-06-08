package de.julielab.jcore.pipeline.builder.cli.menu;

import java.util.function.Function;

/**
 * <p>A MenuItem that carries an arbitrary object as payload for later retrieval upon the selected item.</p>
 * @param <T> The class of the payload object.
 */
public class PayloadMenuItem<T> implements IMenuItem {
    private final T payload;
    private final Function<T, String> nameFunc;

    public PayloadMenuItem(T payload, Function<T, String> nameFunc) {
        this.payload = payload;
        this.nameFunc = nameFunc;
    }

    public T getPayload() {
        return payload;
    }

    @Override
    public String getName() {
        return nameFunc.apply(payload);
    }

    @Override
    public String toString() {
        return getName();
    }
}
