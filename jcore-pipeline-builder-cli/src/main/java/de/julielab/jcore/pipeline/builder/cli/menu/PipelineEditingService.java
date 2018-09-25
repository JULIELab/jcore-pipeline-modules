package de.julielab.jcore.pipeline.builder.cli.menu;

import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;

public class PipelineEditingService {

    private static PipelineEditingService service;

    public static final PipelineEditingService getInstance() {
        if (service == null)
            service = new PipelineEditingService();
        return service;
    }

    private PipelineEditingService() {
    }

    public void edit(JCoReUIMAPipeline pipeline, EditMenuItem editingItem) {

    }
}
