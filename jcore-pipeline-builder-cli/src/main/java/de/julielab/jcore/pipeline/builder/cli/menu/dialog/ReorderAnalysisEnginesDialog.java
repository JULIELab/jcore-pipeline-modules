package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;

public class ReorderAnalysisEnginesDialog extends ReorderComponentsDialog {
    public ReorderAnalysisEnginesDialog() {
        super(PipelineBuilderConstants.JcoreMeta.Category.ae, "Reorder Analysis Engines", JCoReUIMAPipeline::getAeDelegates);
    }
}
