package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.main.Description;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.builder.cli.menu.ArtifactVersionMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.TerminalPrefixes;
import de.julielab.jcore.pipeline.builder.cli.util.TextIOUtils;
import de.julielab.utilities.aether.AetherUtilities;
import de.julielab.utilities.aether.MavenArtifact;
import de.julielab.utilities.aether.MavenException;
import org.beryx.textio.TextIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class UpdateAllArtifactsDialog implements IMenuDialog {
    private final static Logger log = LoggerFactory.getLogger(UpdateAllArtifactsDialog.class);

    public void execute(JCoReUIMAPipeline pipeline, TextIO textIO) {
        List<Description> itemList = new ArrayList<>();
        if (pipeline.getCrDescription() != null)
            itemList.add(pipeline.getCrDescription());
        if (pipeline.getCmDelegates() != null && !pipeline.getCmDelegates().isEmpty()) {
            itemList.addAll(pipeline.getCmDelegates());
        }
        if (pipeline.getAeDelegates() != null && !pipeline.getAeDelegates().isEmpty()) {
            itemList.addAll(pipeline.getAeDelegates());
        }
        if (pipeline.getCcDelegates() != null && !pipeline.getCcDelegates().isEmpty())
            itemList.addAll(pipeline.getCcDelegates());

        if (itemList.isEmpty())
            textIO.getTextTerminal().executeWithPropertiesPrefix(TerminalPrefixes.WARN, t -> t.print("No components selected yet."));

        String updateAll = "Update all component artifacts to their newest available version";
        String selectManually = "Select the version for each component manually";
        String back = "Back";
        String response = textIO.newStringInputReader()
                .withNumberedPossibleValues(updateAll, selectManually, back)
                .withDefaultValue(back)
                .read("Would you like to update all components at once or select a specific version for each component individually?");

        if (updateAll.equals(response)) {
            for (Description description : itemList) {
                if (description.getMetaDescription().isPear()) {
                    String msg = "Description \"" + description.getName() + "\" is a PEAR and cannot be updated automatically.";
                    textIO.getTextTerminal().print(msg + "\n");
                    log.info(msg);
                    continue;
                }
                try {
                    String newestVersion = AetherUtilities.getNewestVersion(description.getMetaDescription().getMavenArtifactCoordinates());
                    if (newestVersion != null && ! newestVersion.isBlank()) {
                        description.getMetaDescription().getMavenArtifactCoordinates().setVersion(newestVersion);
                        textIO.getTextTerminal().print("Set artifact version of component " + description.getName() + " to " + newestVersion + System.getProperty("line.separator"));
                    } else {
                        textIO.getTextTerminal().print("Could not obtain any version for component " + description.getName() + ", retaining current version." + System.getProperty("line.separator"));
                    }
                } catch (MavenException e) {
                    log.error("Could not set the new version to component {}", description.getName(), e);
                }
            }
        } else {
            Set<String> alreadySpecifiedArtifacts = new HashSet<>();
            for (Description description : itemList) {
                MavenArtifact coordinates = description.getMetaDescription().getMavenArtifactCoordinates();
                if (alreadySpecifiedArtifacts.add(coordinates.getGroupId()+":"+coordinates.getArtifactId()))
                    new ArtifactVersionMenuItem(description).selectVersion(textIO, pipeline);
                else
                    // We can just skip the component and don't need to set the correct version because that already happened in the ArtifactVersionMenuItem on first occurrence of the current artifact.
                    TextIOUtils.printLines(Stream.of(TextIOUtils.createPrintLine("Skipping component " + description.getName() + " because the version of the artifact " + coordinates.getGroupId() + ":" + coordinates.getArtifactId() + " has already selected for another component." + System.getProperty("line.separator"), TerminalPrefixes.EMPHASIS)), textIO);
            }
        }
    }

    @Override
    public String getName() {
        return "Update All Artifacts";
    }

    @Override
    public String toString() {
        return getName();
    }
}
