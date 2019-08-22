package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants;
import de.julielab.jcore.pipeline.builder.base.main.Description;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.builder.cli.menu.*;
import de.julielab.jcore.pipeline.builder.cli.util.MenuItemExecutionException;
import de.julielab.jcore.pipeline.builder.cli.util.StatusPrinter;
import org.beryx.textio.TextIO;

import java.util.Deque;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class ReorderComponentsDialog extends AbstractComponentSelectionDialog {
    private PipelineBuilderConstants.JcoreMeta.Category componentCategory;
    private String dialogName;
    private Function<JCoReUIMAPipeline, List<Description>> delegateSupplier;

    public ReorderComponentsDialog(PipelineBuilderConstants.JcoreMeta.Category componentCategory, String dialogName, Function<JCoReUIMAPipeline, List<Description>> delegateSupplier) {
        this.componentCategory = componentCategory;
        this.dialogName = dialogName;
        this.delegateSupplier = delegateSupplier;
    }

    @Override
    public IMenuItem executeMenuItem(JCoReUIMAPipeline pipeline, TextIO textIO, Deque<String> path) {
        init(pipeline, EnumSet.of(componentCategory));
        printPosition(textIO, path);
        StatusPrinter.printPipelineStatus(pipeline, textIO);
        IMenuItem choice = textIO.<IMenuItem>newGenericInputReader(null)
                .withNumberedPossibleValues(itemList).withDefaultValue(BackMenuItem.get())
                .read("\nChoose a component to move.");
        if (!(choice instanceof BackMenuItem)) {
            MenuItemList<IMenuItem> positionItems = new MenuItemList<>();
            itemList.stream().filter(i -> !(i instanceof BackMenuItem)).forEach(positionItems::add);
            positionItems.add(new PositionMenuItem("<Move to back>", itemList.size()));
            positionItems.add(BackMenuItem.get());
            IMenuItem choice2 = textIO.<IMenuItem>newGenericInputReader(null)
                    .withNumberedPossibleValues(positionItems).withDefaultValue(BackMenuItem.get())
                    .read("\nChoose the position to move the component before.");
            if (!(choice2 instanceof BackMenuItem)) {
                List<Description> componentDescriptions = delegateSupplier.apply(pipeline);
                ComponentSelectionMenuItem selection1 = (ComponentSelectionMenuItem) choice;
                ComponentSelectionMenuItem selection2 = null;
                if (!(choice2 instanceof PositionMenuItem))
                    selection2 = (ComponentSelectionMenuItem) choice2;
                int index1 = componentDescriptions.indexOf(selection1.getDescription());
                int index2 = selection2 != null ? componentDescriptions.indexOf(selection2.getDescription()) : componentDescriptions.size();
                if (index1 < index2) {
                    for (int i = index1; i < index2 && i < componentDescriptions.size()-1; i++)
                        componentDescriptions.set(i, componentDescriptions.get(i + 1));
                componentDescriptions.set(index2-1, selection1.getDescription());
                } else if (index1 > index2) {
                    for (int i = index1; i > index2; i--)
                        componentDescriptions.set(i, componentDescriptions.get(i - 1));
                    componentDescriptions.set(index2, selection1.getDescription());
                }
            } else {
                choice = choice2;
            }
        }
        clearTerminal(textIO);
        return choice;
    }

    @Override
    public String getName() {
        return dialogName;
    }
}
