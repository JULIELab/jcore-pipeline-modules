package de.julielab.jcore.pipeline.builder.cli.menu;

public class PositionMenuItem implements IMenuItem {

    private final String name;
    private final int position;

    public PositionMenuItem(String name, int position) {
        this.name = name;
        this.position = position;
    }

    @Override
    public String getName() {
        return name;
    }

    public int getPosition() {
        return position;
    }

    @Override
    public String toString() {
        return getName();
    }
}
