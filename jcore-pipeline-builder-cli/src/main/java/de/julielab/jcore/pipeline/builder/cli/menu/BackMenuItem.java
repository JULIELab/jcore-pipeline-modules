package de.julielab.jcore.pipeline.builder.cli.menu;

public class BackMenuItem implements IMenuItem {
    @Override
    public String getName() {
        return "Back";
    }

    @Override
    public  String toString() {
        return getName();
    }
}
