package de.julielab.jcore.pipeline.builder.cli.menu;

import de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants;
import de.julielab.jcore.pipeline.builder.base.main.MetaDescription;

/**
 * For adding or removing components form the pipeline.
 */
public class EditMenuItem implements IMenuItem {
    public MetaDescription getDescription() {
        return description;
    }

    private final MetaDescription description;

    public EditMenuItem(MetaDescription description, PipelineBuilderConstants.JcoreMeta.Category category) {
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
