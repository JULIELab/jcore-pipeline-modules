package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.builder.cli.menu.*;
import de.julielab.jcore.pipeline.builder.cli.util.MenuItemExecutionException;
import org.beryx.textio.TextIO;

import java.util.Deque;
import java.util.EnumSet;

public class ArtifactVersionDialog extends AbstractComponentSelectionDialog {

    @Override
    public String getName() {
        return "Adapt Component Artifact Versions";
    }

    @Override
    public IMenuItem executeMenuItem(JCoReUIMAPipeline pipeline, TextIO textIO, Deque<String> path) throws MenuItemExecutionException {
        IMenuItem item = super.executeMenuItem(pipeline, textIO, path);
        if (item instanceof ComponentSelectionMenuItem) {
            ComponentSelectionMenuItem componentItem = (ComponentSelectionMenuItem) item;
            new ArtifactVersionMenuItem(componentItem.getDescription()).selectVersion(textIO, pipeline);
        } else if (item instanceof UpdateAllArtifactsDialog) {
            UpdateAllArtifactsDialog dialog = (UpdateAllArtifactsDialog) item;
            dialog.execute(pipeline, textIO);
        }
        return item;
    }

    @Override
    protected void init(JCoReUIMAPipeline pipeline, EnumSet<PipelineBuilderConstants.JcoreMeta.Category> categories) {
        super.init(pipeline, categories);
        MenuItemList<IMenuItem> extendedList = new MenuItemList<>();
        extendedList.add(new UpdateAllArtifactsDialog());
        extendedList.addAll(itemList);
        itemList = extendedList;
    }
}
