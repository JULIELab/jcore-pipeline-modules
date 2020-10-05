package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.connectors.GitHubConnector;
import de.julielab.jcore.pipeline.builder.base.connectors.RepositoryBranchInformation;
import de.julielab.jcore.pipeline.builder.base.main.ComponentRepository;
import de.julielab.jcore.pipeline.builder.base.main.GitHubRepository;
import org.beryx.textio.TextIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;

public class RepositorySelectVersionDialog implements IMenuDialog {
private final static Logger log = LoggerFactory.getLogger(RepositorySelectVersionDialog.class);
    private final ComponentRepository repository;

    public RepositorySelectVersionDialog(ComponentRepository repository) {
        this.repository = repository;
    }

    @Override
    public String getName() {
        return "Select version for repository '" + repository.getName() + "'";
    }

    @Override
    public String toString() {
        return getName();
    }

    public void execute(TextIO textIO, Deque<String> path) {
        clearTerminal(textIO);
        path.add(getName());
        printPosition(textIO, path);

        if (repository instanceof GitHubRepository) {
            GitHubRepository ghr = (GitHubRepository) repository;
            final List<RepositoryBranchInformation> repositoryBranches = GitHubConnector.getRepositoryBranches(ghr);
            List<String> branchNames = repositoryBranches.stream().map(RepositoryBranchInformation::getName).collect(Collectors.toList());
            final String chosenBranchName = textIO
                    .<String>newGenericInputReader(null)
                    .withNumberedPossibleValues(branchNames)
                    .read("Select the repository branch to retrieve the components from.");

            repository.setVersion(chosenBranchName);
        }

        path.pop();
    }
}
