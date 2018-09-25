package de.julielab.jcore.pipeline.builder.cli.menu;

import org.apache.uima.resource.ExternalResourceDependency;

public class ExternalResourceDependencyDefinitionMenuItem implements IMenuItem {

    public ExternalResourceDependencyDefinitionMenuItem() {
    }

    @Override
    public String getName() {
        return "Define new External Resource Dependency";
    }

    @Override
    public String toString() {
        return getName();
    }

}
