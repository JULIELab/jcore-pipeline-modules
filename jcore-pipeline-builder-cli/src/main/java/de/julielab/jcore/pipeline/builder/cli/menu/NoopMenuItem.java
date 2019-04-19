package de.julielab.jcore.pipeline.builder.cli.menu;

public class NoopMenuItem implements IMenuItem {
    @Override
    public String getName() {
        return "Do Nothing";
    }

    @Override
    public  String toString() {
        return getName();
    }
}
