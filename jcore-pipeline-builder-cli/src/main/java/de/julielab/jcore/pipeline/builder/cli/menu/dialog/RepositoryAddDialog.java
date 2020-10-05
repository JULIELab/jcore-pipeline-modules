package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import com.google.common.collect.Sets;
import de.julielab.jcore.pipeline.builder.base.main.ComponentRepository;
import de.julielab.jcore.pipeline.builder.base.main.Repositories;
import de.julielab.jcore.pipeline.builder.cli.menu.BackMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.IMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.NoopMenuItem;
import org.beryx.textio.TextIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class RepositoryAddDialog implements ILoopableDialog {
    private final static Logger log = LoggerFactory.getLogger(RepositoryAddDialog.class);

    @Override
    public IMenuItem executeMenuItem(TextIO textIO, Deque<String> path) {
        path.add("Add a repository");
        clearTerminal(textIO);
        printPosition(textIO, path);
        try {

            final TreeSet<ComponentRepository> currentlyUsed = Repositories.getRepositories().collect(Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing(ComponentRepository::getName))));
            final TreeSet<ComponentRepository> defaultRepositories = Repositories.JCORE_REPOSITORIES.stream().collect(Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing(ComponentRepository::getName))));
            final Sets.SetView<ComponentRepository> unusedDefaultRepositories = Sets.difference(defaultRepositories, currentlyUsed);
            List<Object> items = new ArrayList<>(unusedDefaultRepositories);
            items.add(new RepositoryCreateDialog());
            items.add(BackMenuItem.get());

            List<String> menuItemStrings = items.stream().map(item -> {
                if (item instanceof ComponentRepository)
                    return ((ComponentRepository) item).getName();
                return item.toString();
            }).collect(Collectors.toList());

            final String chosenString = textIO.<String>newGenericInputReader(null)
                    .withNumberedPossibleValues(menuItemStrings)
                    .read("\nChoose an option to add a default repository or create a new one.");

            Object choice = items.get(menuItemStrings.indexOf(chosenString));

            try {
                if (choice instanceof ComponentRepository) {
                    ComponentRepository repo = (ComponentRepository) choice;
                    if (repo.getVersion() == null) {
                        new RepositorySelectVersionDialog(repo).execute(textIO, path);
                        Repositories.addRepositories(repo);
                    }
                    return new NoopMenuItem();
                } else if (choice instanceof RepositoryCreateDialog) {

                }
            } catch (IOException e) {
                log.error("Could not add the chosen repository", e);
            }

            return (IMenuItem) choice;
        } finally {
            path.pop();
        }
    }

    @Override
    public String getName() {
        return "Add Component Repositories";
    }

    @Override
    public String toString() {
        return getName();
    }
}
