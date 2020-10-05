package de.julielab.jcore.pipeline.builder.cli.menu;

public class BackMenuItem implements IMenuItem {
    private static final BackMenuItem item = new BackMenuItem();

    private BackMenuItem() {
    }

    public static BackMenuItem get() {
        return item;
    }

    @Override
    public String getName() {
        return "Back";
    }

    @Override
    public String toString() {
        return getName();
    }
}
