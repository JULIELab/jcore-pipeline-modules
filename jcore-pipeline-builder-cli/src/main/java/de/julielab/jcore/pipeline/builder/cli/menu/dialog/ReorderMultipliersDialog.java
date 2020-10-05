package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;

public class ReorderMultipliersDialog extends ReorderComponentsDialog {
    public ReorderMultipliersDialog() {
        super(PipelineBuilderConstants.JcoreMeta.Category.multiplier, "Reorder Multipliers", JCoReUIMAPipeline::getCmDelegates);
    }
}
