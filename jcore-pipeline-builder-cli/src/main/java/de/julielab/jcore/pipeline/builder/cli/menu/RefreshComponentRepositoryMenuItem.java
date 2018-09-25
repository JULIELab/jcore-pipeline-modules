package de.julielab.jcore.pipeline.builder.cli.menu;

import de.julielab.jcore.pipeline.builder.cli.menu.IMenuItem;

public class RefreshComponentRepositoryMenuItem implements IMenuItem {
    @Override
    public String getName() {
        return "Refresh Component Repository";
    }

    @Override
    public String toString() {
        return getName();
    }
}
