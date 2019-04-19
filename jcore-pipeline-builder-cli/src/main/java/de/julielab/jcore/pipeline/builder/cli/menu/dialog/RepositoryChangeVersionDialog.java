package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.main.ComponentRepository;
import de.julielab.jcore.pipeline.builder.base.main.JcoreGithubInformationService;
import de.julielab.jcore.pipeline.builder.base.main.Repositories;
import de.julielab.jcore.pipeline.builder.cli.menu.*;
import org.beryx.textio.TextIO;

import java.util.Deque;

public class RepositoryChangeVersionDialog implements ILoopableDialog {
    @Override
    public IMenuItem executeMenuItem(TextIO textIO, Deque<String> path) {
        final IMenuItem choice = new RepositorySelectionDialog().selectRepository(textIO, path);
        if (choice instanceof BackMenuItem)
            return choice;
        ComponentRepository chosenRepo = ((PayloadMenuItem<ComponentRepository>) choice).getPayload();
        if (!chosenRepo.isUpdateable()) {
            textIO.getTextTerminal().executeWithPropertiesPrefix(TerminalPrefixes.ERROR, t -> t.print("The repository " + chosenRepo.getName() + " cannot be updated according to its settings."));
            return new NoopMenuItem();
        }
        String oldVersion = chosenRepo.getVersion();
        new RepositorySelectVersionDialog(chosenRepo).execute(textIO, path);
        String newVersion = chosenRepo.getVersion();
        if (oldVersion != null && !oldVersion.equals(newVersion))
            Repositories.deleteComponentList(chosenRepo.getName(), oldVersion);
        return new NoopMenuItem();
    }

    @Override
    public String getName() {
        return "Change Repository Version";
    }

    @Override
    public String toString() {
        return getName();
    }
}
