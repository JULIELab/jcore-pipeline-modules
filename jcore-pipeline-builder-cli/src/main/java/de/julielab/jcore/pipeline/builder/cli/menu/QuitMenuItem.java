package de.julielab.jcore.pipeline.builder.cli.menu;

public class QuitMenuItem implements IMenuItem {
    @Override
    public String getName() {
        return "Quit";
    }

    @Override
    public String toString() {
        return getName();
    }
}
