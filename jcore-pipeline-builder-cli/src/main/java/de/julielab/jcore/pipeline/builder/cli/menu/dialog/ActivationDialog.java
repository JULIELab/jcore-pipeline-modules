package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants;
import de.julielab.jcore.pipeline.builder.base.main.Description;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.builder.cli.main.PipelineBuilderCLI;
import de.julielab.jcore.pipeline.builder.cli.menu.BackMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.ComponentSelectionMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.IMenuItem;
import de.julielab.jcore.pipeline.builder.cli.util.MenuItemExecutionException;
import de.julielab.jcore.pipeline.builder.cli.util.StatusPrinter;
import org.beryx.textio.TextIO;

import java.util.Deque;
import java.util.EnumSet;

public class ActivationDialog extends AbstractComponentSelectionDialog {
    @Override
    public IMenuItem executeMenuItem(JCoReUIMAPipeline pipeline, TextIO textIO, Deque<String> path) throws MenuItemExecutionException {
        init(pipeline, EnumSet.of(PipelineBuilderConstants.JcoreMeta.Category.multiplier, PipelineBuilderConstants.JcoreMeta.Category.ae, PipelineBuilderConstants.JcoreMeta.Category.consumer, PipelineBuilderConstants.JcoreMeta.Category.flowcontroller));
        printPosition(textIO, path);
        StatusPrinter.printPipelineStatus(pipeline, PipelineBuilderCLI.statusVerbosity, textIO);
        IMenuItem choice = textIO.<IMenuItem>newGenericInputReader(null)
                .withNumberedPossibleValues(itemList).withDefaultValue(BackMenuItem.get())
                .read("\nChoose a component.");
        clearTerminal(textIO);
        if (choice instanceof ComponentSelectionMenuItem) {
            final Description description = ((ComponentSelectionMenuItem) choice).getDescription();
            description.setActive(!description.isActive());
        } else if (choice instanceof ActivateAllMenuItem) {
            itemList.stream()
                    .filter(ComponentSelectionMenuItem.class::isInstance)
                    .map(ComponentSelectionMenuItem.class::cast)
                    .map(ComponentSelectionMenuItem::getDescription)
                    .forEach(d -> d.setActive(true));
        } else if (choice instanceof DeactivateAllMenuItem) {
            itemList.stream()
                    .filter(ComponentSelectionMenuItem.class::isInstance)
                    .map(ComponentSelectionMenuItem.class::cast)
                    .map(ComponentSelectionMenuItem::getDescription)
                    .forEach(d -> d.setActive(false));
        }
        return choice;
    }

    @Override
    protected void init(JCoReUIMAPipeline pipeline, EnumSet<PipelineBuilderConstants.JcoreMeta.Category> categories) {
        super.init(pipeline, categories);
        // remove the back item, we will append it to the end again
        itemList.remove(itemList.size() - 1);
        itemList.add(new ActivateAllMenuItem());
        itemList.add(new DeactivateAllMenuItem());
        itemList.add(BackMenuItem.get());
    }

    @Override
    public String getName() {
        return "Manage Component Activation Status";
    }

    private static class ActivateAllMenuItem implements IMenuItem {
        @Override
        public String getName() {
            return "Activate all";
        }

        @Override
        public String toString() {
            return getName();
        }
    }

    private static class DeactivateAllMenuItem implements IMenuItem {
        @Override
        public String getName() {
            return "Deactivate all";
        }

        @Override
        public String toString() {
            return getName();
        }
    }

}
