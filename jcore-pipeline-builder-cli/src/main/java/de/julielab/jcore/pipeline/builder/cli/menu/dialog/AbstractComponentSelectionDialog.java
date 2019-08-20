package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants.JcoreMeta.Category;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.builder.cli.menu.BackMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.ComponentSelectionMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.IMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.MenuItemList;
import de.julielab.jcore.pipeline.builder.cli.util.MenuItemExecutionException;
import de.julielab.jcore.pipeline.builder.cli.util.StatusPrinter;
import org.beryx.textio.TextIO;

import java.util.Deque;
import java.util.EnumSet;
import java.util.Set;

public abstract class AbstractComponentSelectionDialog implements ILoopablePipelineManipulationDialog {

    protected MenuItemList<IMenuItem> itemList;

    protected void init(JCoReUIMAPipeline pipeline, EnumSet<Category> categories) {
        Set<Category> categoriesForSelection = EnumSet.allOf(Category.class);
        if (!categories.isEmpty())
            categoriesForSelection = categories;
        itemList = new MenuItemList<>();
        if (pipeline.getCrDescription() != null && categoriesForSelection.contains(Category.reader))
            itemList.add(new ComponentSelectionMenuItem(pipeline.getCrDescription()));
        if (pipeline.getCmDelegates() != null && !pipeline.getCmDelegates().isEmpty() && categoriesForSelection.contains(Category.multiplier)) {
            pipeline.getCmDelegates().stream().map(ComponentSelectionMenuItem::new).forEach(itemList::add);
        }
        if (pipeline.getAeDelegates() != null && !pipeline.getAeDelegates().isEmpty() && categoriesForSelection.contains(Category.ae)) {
            pipeline.getAeDelegates().stream().map(ComponentSelectionMenuItem::new).forEach(itemList::add);
        }
        if (pipeline.getCcDelegates() != null && !pipeline.getCcDelegates().isEmpty() && categoriesForSelection.contains(Category.consumer))
            pipeline.getCcDelegates().stream().map(ComponentSelectionMenuItem::new).forEach(itemList::add);
        itemList.add(BackMenuItem.get());
    }

    @Override
    public IMenuItem executeMenuItem(JCoReUIMAPipeline pipeline, TextIO textIO, Deque<String> path) throws MenuItemExecutionException {
        init(pipeline, EnumSet.allOf(Category.class));
        printPosition(textIO, path);
        StatusPrinter.printPipelineStatus(pipeline, textIO);
        IMenuItem choice = textIO.<IMenuItem>newGenericInputReader(null)
                .withNumberedPossibleValues(itemList).withDefaultValue(BackMenuItem.get())
                .read("\nChoose a component.");
        clearTerminal(textIO);
        return choice;
    }

    @Override
    public String toString() {
        return getName();
    }

}
