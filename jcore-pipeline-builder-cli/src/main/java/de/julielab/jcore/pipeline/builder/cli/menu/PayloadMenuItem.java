package de.julielab.jcore.pipeline.builder.cli.menu;

import java.util.function.Function;

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
