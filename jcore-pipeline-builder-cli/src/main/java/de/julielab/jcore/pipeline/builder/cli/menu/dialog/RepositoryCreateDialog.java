package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

public class RepositoryCreateDialog implements IMenuDialog{
    @Override
    public String getName() {
        return "Create new Component Repository";
    }

    @Override
    public String toString() {
        return getName();
    }
}
