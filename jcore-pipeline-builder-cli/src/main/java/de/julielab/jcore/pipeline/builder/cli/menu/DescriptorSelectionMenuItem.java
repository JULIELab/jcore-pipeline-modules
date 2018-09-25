package de.julielab.jcore.pipeline.builder.cli.menu;

import de.julielab.jcore.pipeline.builder.base.main.Description;

public class DescriptorSelectionMenuItem implements IMenuItem {

    private Description description;

    public Description getDescription() {
        return description;
    }

    public void setDescription(Description description) {
        this.description = description;
    }

    public DescriptorSelectionMenuItem(Description description) {
        this.description = description;
    }

    @Override
    public String getName() {
        return description.getName();
    }

    @Override
    public String toString() {
        return getName();
    }


}
