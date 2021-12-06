package de.julielab.jcore.pipeline.builder.cli.menu;

import de.julielab.jcore.pipeline.builder.base.main.Description;

public class ComponentSelectionMenuItem implements IMenuItem {

    private final Description description;
    private String name;

    public ComponentSelectionMenuItem(Description description) {
        this.description = description;
    }

    public Description getDescription() {
        return description;
    }

    @Override
    public String getName() {
        return description.isActive() ?  description.getName() : description.getName() + " (inactive)";
    }

    @Override
    public String toString() {
        return getName();
    }

}
