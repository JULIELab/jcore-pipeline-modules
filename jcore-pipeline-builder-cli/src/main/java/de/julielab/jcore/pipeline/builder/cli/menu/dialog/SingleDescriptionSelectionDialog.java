package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.exceptions.DescriptorLoadingException;
import de.julielab.jcore.pipeline.builder.base.main.Description;
import de.julielab.jcore.pipeline.builder.base.main.MetaDescription;
import de.julielab.jcore.pipeline.builder.cli.menu.DescriptorSelectionMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.IMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.MenuItemList;
import org.beryx.textio.TextIO;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SingleDescriptionSelectionDialog implements IMenuDialog {
    private final MenuItemList<DescriptorSelectionMenuItem> menuItems;
    private MetaDescription metaDescription;

    public SingleDescriptionSelectionDialog(MetaDescription metaDescription, Predicate<Description> descriptionFilter) throws DescriptorLoadingException {
        this.metaDescription = metaDescription;
        menuItems = metaDescription.getJCoReDescriptions()
                .stream().filter(descriptionFilter).map(DescriptorSelectionMenuItem::new).sorted()
                .collect(Collectors.toCollection(MenuItemList::new));
    }

    public void chooseDescription(TextIO textIO, Deque<String> path) throws DescriptorLoadingException {

        // this may happen after filtering
        if (menuItems.size() == 1) {
            metaDescription.setChosenDescriptor(menuItems.get(0).getDescription().getLocation());
        } else {
            path.add(getName());
            printPosition(textIO, path);
            DescriptorSelectionMenuItem choice = textIO.<DescriptorSelectionMenuItem>newGenericInputReader(null)
                    .withNumberedPossibleValues(menuItems)
                    .read("\nChoose a specific descriptor.");
            clearTerminal(textIO);
            metaDescription.setChosenDescriptor(choice.getDescription().getLocation());
            path.removeLast();
        }
    }


    @Override
    public String getName() {
        return "Choose Descriptor";
    }
}
