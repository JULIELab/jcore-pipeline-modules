package de.julielab.jcore.pipeline.builder.cli.menu;

public interface IMenuItem extends Comparable<IMenuItem> {
    String getName();

    @Override
    default int compareTo(IMenuItem o) {
        return getName().compareTo(o.getName());
    }

}
