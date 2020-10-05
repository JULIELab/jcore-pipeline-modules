package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.main.ComponentRepository;
import de.julielab.jcore.pipeline.builder.base.main.Repositories;
import de.julielab.jcore.pipeline.builder.cli.menu.BackMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.IMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.MenuItemList;
import org.beryx.textio.TextIO;

import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;

public class RepositoryManagementDialog implements ILoopableDialog {


    private final MenuItemList<IMenuItem> itemList;

    public RepositoryManagementDialog() {
        itemList = new MenuItemList<>();
        itemList.add(new RepositoryAddDialog());
        itemList.add(new RepositoryChangeVersionDialog());
        itemList.add(BackMenuItem.get());
    }

    @Override
    public String getName() {
        return "Manage Component Repositories";
    }

    @Override
    public IMenuItem executeMenuItem(TextIO textIO, Deque<String> path) {
        final List<ComponentRepository> repositories = Repositories.getRepositories().collect(Collectors.toList());
        final String ls = System.getProperty("line.separator");
        textIO.getTextTerminal().print("The following repositories are currently in use:" + ls);
        for (ComponentRepository repository : repositories) {
            textIO.getTextTerminal().print(repository.getName() + ": " + repository.getVersion() + ls);
        }
        final IMenuItem choice = textIO.<IMenuItem>newGenericInputReader(null)
                .withNumberedPossibleValues(itemList).withDefaultValue(BackMenuItem.get())
                .read("Choose an option:");
        if (choice instanceof ILoopableDialog) {
            ((ILoopableDialog) choice).enterInputLoop(textIO, path);
        }
        return choice;
    }

    @Override
    public String toString() {
        return getName();
    }
}
