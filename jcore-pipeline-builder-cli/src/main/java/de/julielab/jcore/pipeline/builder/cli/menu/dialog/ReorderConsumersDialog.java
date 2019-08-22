package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;

public class ReorderConsumersDialog extends ReorderComponentsDialog {
    public ReorderConsumersDialog() {
        super(PipelineBuilderConstants.JcoreMeta.Category.consumer, "Reorder Consumers", JCoReUIMAPipeline::getCcDelegates);
    }
}
