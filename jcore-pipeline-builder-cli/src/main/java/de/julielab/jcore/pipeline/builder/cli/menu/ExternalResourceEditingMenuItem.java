package de.julielab.jcore.pipeline.builder.cli.menu;

import org.apache.uima.resource.ExternalResourceDependency;

public class ExternalResourceEditingMenuItem implements IMenuItem {
    private ExternalResourceDependency dependency;

    public ExternalResourceEditingMenuItem(ExternalResourceDependency dependency) {
        this.dependency = dependency;
    }

    @Override
    public String getName() {
        return dependency.getKey();
    }

    @Override
    public String toString() {
        return getName();
    }

    public ExternalResourceDependency getDependency() {
        return dependency;
    }
}
