package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.builder.cli.menu.BackMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.IMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.MenuItemList;
import de.julielab.jcore.pipeline.builder.cli.util.MenuItemExecutionException;
import de.julielab.jcore.pipeline.builder.cli.util.StatusPrinter;
import org.beryx.textio.TextIO;

import java.util.Deque;

public class ConfigurePipelineDialog implements ILoopablePipelineManipulationDialog {

    private MenuItemList<IMenuItem> itemList;

    @Override
    public IMenuItem executeMenuItem(JCoReUIMAPipeline pipeline, TextIO textIO, Deque<String> path) throws MenuItemExecutionException {
        init(pipeline);
        printPosition(textIO, path);
        StatusPrinter.printPipelineStatus(pipeline, textIO);
        IMenuItem choice = textIO.<IMenuItem>newGenericInputReader(null)
                .withNumberedPossibleValues(itemList)
                .read("\nChoose an action to perform on your pipeline.");
        clearTerminal(textIO);
        if (!(choice instanceof BackMenuItem)) {
            ILoopablePipelineManipulationDialog dialog = (ILoopablePipelineManipulationDialog) choice;
            dialog.enterInputLoop(pipeline, textIO, path);
        }
        clearTerminal(textIO);
        return choice;
    }

    private void init(JCoReUIMAPipeline pipeline) {
        itemList = new MenuItemList<>();
        itemList.add(new ComponentConfigurationSelectionDialog());
        itemList.add(new ReorderComponentsDialog());
        itemList.add(new RemoveComponentDialog());
        itemList.add(new ArtifactVersionDialog());
        itemList.add(new BackMenuItem());

    }

    @Override
    public String getName() {
        return "Configure Pipeline";
    }

    @Override
    public String toString() {
        return getName();
    }
}
