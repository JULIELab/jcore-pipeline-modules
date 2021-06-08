package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.builder.base.main.MetaDescription;
import de.julielab.jcore.pipeline.builder.cli.main.PipelineBuilderCLI;
import de.julielab.jcore.pipeline.builder.cli.menu.BackMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.IMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.MenuItemList;
import de.julielab.jcore.pipeline.builder.cli.util.MenuItemExecutionException;
import de.julielab.jcore.pipeline.builder.cli.util.StatusPrinter;
import org.beryx.textio.TextIO;

import java.util.Deque;
import java.util.List;
import java.util.Map;

public class AddFlowControllerDialog implements ILoopablePipelineManipulationDialog {

    public static final String ANALYSIS_ENGINE_AGGREGATE = "Analysis Engine Aggregate";
    public static final String CAS_CONSUMER_AGGREGATE = "CAS Consumer Aggregate";
    private MenuItemList<IMenuItem> itemList;
    private Map<PipelineBuilderConstants.JcoreMeta.Category, List<MetaDescription>> categoryMap;

    public AddFlowControllerDialog(Map<PipelineBuilderConstants.JcoreMeta.Category, List<MetaDescription>> categoryMap) {

        this.categoryMap = categoryMap;
    }

    @Override
    public IMenuItem executeMenuItem(JCoReUIMAPipeline pipeline, TextIO textIO, Deque<String> path) throws MenuItemExecutionException {
        init();
        printPosition(textIO, path);
        StatusPrinter.printPipelineStatus(pipeline, PipelineBuilderCLI.statusVerbosity, textIO);
        IMenuItem choice = textIO.<IMenuItem>newGenericInputReader(null)
                .withNumberedPossibleValues(itemList).withDefaultValue(BackMenuItem.get())
                .read("\nChoose an action to perform on your pipeline.");
        clearTerminal(textIO);
        if (!(choice instanceof BackMenuItem)) {
            ILoopablePipelineManipulationDialog dialog = (ILoopablePipelineManipulationDialog) choice;
            dialog.enterInputLoop(pipeline, textIO, path);
        }
        clearTerminal(textIO);
        return choice;
    }

    private void init() {
        itemList = new MenuItemList<>();
        itemList.add(new AddComponentDialog(categoryMap, PipelineBuilderConstants.JcoreMeta.Category.flowcontroller) {
            @Override
            public String getName() {
                return ANALYSIS_ENGINE_AGGREGATE;
            }
        });
        itemList.add(new AddComponentDialog(categoryMap, PipelineBuilderConstants.JcoreMeta.Category.flowcontroller) {
            @Override
            public String getName() {
                return CAS_CONSUMER_AGGREGATE;
            }
        });
        itemList.add(BackMenuItem.get());

    }

    @Override
    public String getName() {
        return "Flow Controller";
    }

    @Override
    public String toString() {
        return getName();
    }
}
