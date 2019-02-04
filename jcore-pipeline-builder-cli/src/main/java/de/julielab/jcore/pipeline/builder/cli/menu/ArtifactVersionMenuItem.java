package de.julielab.jcore.pipeline.builder.cli.menu;

import de.julielab.java.utilities.prerequisites.PrerequisiteChecker;
import de.julielab.jcore.pipeline.builder.base.main.Description;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.utilities.aether.AetherUtilities;
import de.julielab.utilities.aether.MavenArtifact;
import de.julielab.utilities.aether.MavenException;
import org.beryx.textio.TextIO;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ArtifactVersionMenuItem implements IMenuItem {
    private Description description;

    public ArtifactVersionMenuItem(Description description) {
        this.description = description;
    }

    public void selectVersion(TextIO textIO, JCoReUIMAPipeline pipeline) {
        PrerequisiteChecker.checkThat()
                .notNull(description)
                .supplyNotNull(() -> description.getMetaDescription())
                .supplyNotNull(() -> description.getMetaDescription().getMavenArtifact())
                .withNames("Description", "MetaDescription", "MavenArtifact").execute();

        MavenArtifact artifact = description.getMetaDescription().getMavenArtifact();
        try {
            List<String> versionList = AetherUtilities.getVersions(artifact).collect(Collectors.toList());
            if (versionList.isEmpty()) {
                textIO.getTextTerminal().executeWithPropertiesPrefix(TerminalPrefixes.ERROR, t -> t.print("No versions available for component " + description.getName() + ", Maven artifact " + artifact));
                return;
            }
            // In tests, the received list was sorted ascendingly, we want it descending
            Collections.reverse(versionList);
            String selectedVersion = textIO.newStringInputReader().withNumberedPossibleValues(versionList).withDefaultValue(versionList.get(0)).read("These are the available versions for the component " + description.getName() + ":");

            // Set the very same version to equivalent maven artifacts in all other components (this is mainly the case when a component has multiple instances in the pipeline)
            pipeline.getMavenComponentArtifacts().filter(a -> a.getArtifactId().equalsIgnoreCase(artifact.getArtifactId())
                    && a.getGroupId().equalsIgnoreCase(artifact.getGroupId())
                    && ((a.getClassifier() == null && artifact.getClassifier() == null) || a.getClassifier().equalsIgnoreCase(artifact.getClassifier()))
                    && a.getPackaging().equalsIgnoreCase(artifact.getPackaging())).forEach(a -> a.setVersion(selectedVersion));
        } catch (MavenException e) {
            e.printStackTrace();
        }
    }


    @Override
    public String getName() {
        return description.getName();
    }
}
