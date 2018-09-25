package de.julielab.jcore.pipeline.builder.cli.menu;

import de.julielab.java.utilities.prerequisites.PrerequisiteChecker;
import de.julielab.jcore.pipeline.builder.base.connectors.MavenConnector;
import de.julielab.jcore.pipeline.builder.base.exceptions.MavenException;
import de.julielab.jcore.pipeline.builder.base.main.Description;
import de.julielab.jcore.pipeline.builder.base.main.MavenArtifact;
import de.julielab.jcore.pipeline.builder.cli.menu.dialog.IMenuDialog;
import org.beryx.textio.TextIO;
import org.beryx.textio.TextIoFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ArtifactVersionMenuItem implements IMenuItem {
    private Description description;

    public ArtifactVersionMenuItem(Description description) {
        this.description = description;
    }

    public void selectVersion(TextIO textIO) {
        PrerequisiteChecker.checkThat()
                .notNull(description)
                .supplyNotNull(() -> description.getMetaDescription())
                .supplyNotNull(() -> description.getMetaDescription().getMavenArtifact())
                .withNames("Description", "MetaDescription", "MavenArtifact").execute();

        MavenArtifact artifact = description.getMetaDescription().getMavenArtifact();
        try {
            List<String> versionList = MavenConnector.getVersions(artifact).collect(Collectors.toList());
            if (versionList.isEmpty()) {
                textIO.getTextTerminal().executeWithPropertiesPrefix(TerminalPrefixes.ERROR, t -> t.print("No versions available for component " + description.getName() + ", Maven artifact " + artifact));
                return;
            }
            // In tests, the received list was sorted ascendingly, we want it descending
            Collections.reverse(versionList);
            String selectedVersion = textIO.newStringInputReader().withNumberedPossibleValues(versionList).withDefaultValue(versionList.get(0)).read("These are the available versions for the component " + description.getName() + ":");
            description.getMetaDescription().getMavenArtifact().setVersion(selectedVersion);

        } catch (MavenException e) {
            e.printStackTrace();
        }
    }


    @Override
    public String getName() {
        return description.getName();
    }
}
