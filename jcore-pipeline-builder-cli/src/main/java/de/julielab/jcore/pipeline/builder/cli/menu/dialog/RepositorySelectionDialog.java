package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.main.ComponentRepository;
import de.julielab.jcore.pipeline.builder.base.main.JcoreGithubInformationService;
import de.julielab.jcore.pipeline.builder.base.main.Repositories;
import de.julielab.jcore.pipeline.builder.cli.menu.BackMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.IMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.PayloadMenuItem;
import org.beryx.textio.TextIO;

import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;

public class RepositorySelectionDialog implements IMenuDialog {


    @Override
    public String getName() {
        return "Select Repository";
    }

    /**
     * Returns an <tt>IMenuItem</tt> which is either a <tt>PayloadMenuItem<ComponentRepository</tt> or
     * a <tt>BackMenuItem</tt>.
     * @param textIO The textIO instance.
     * @param path The current path.
     * @return
     */
    public IMenuItem selectRepository(TextIO textIO, Deque<String> path) {
        path.add("Select a repository");
        printPosition(textIO, path);
        final List<IMenuItem> repositories = Repositories.getRepositories().map(r -> new PayloadMenuItem<ComponentRepository>(r, x -> x.getName())).collect(Collectors.toList());
        repositories.add(new BackMenuItem());
        IMenuItem choice = textIO.<IMenuItem>newGenericInputReader(null)
                .withNumberedPossibleValues(repositories)
                .read("\nChoose a repository.");
        path.pop();
        return choice;
    }

    @Override
    public String toString() {
        return getName();
    }
}
