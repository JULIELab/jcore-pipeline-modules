package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.connectors.MavenConnector;
import de.julielab.jcore.pipeline.builder.base.exceptions.MavenException;
import de.julielab.jcore.pipeline.builder.base.main.Description;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.builder.cli.menu.*;
import org.beryx.textio.TextIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class UpdateAllArtifactsDialog implements IMenuDialog {
private final static Logger log = LoggerFactory.getLogger(UpdateAllArtifactsDialog.class);
    public void execute(JCoReUIMAPipeline pipeline, TextIO textIO) {
        List<Description> itemList = new ArrayList<>();
        if (pipeline.getCrDescription() != null)
            itemList.add(pipeline.getCrDescription());
        if (pipeline.getCmDelegates() != null && !pipeline.getCmDelegates().isEmpty()) {
            pipeline.getCmDelegates().stream().forEach(itemList::add);
        }
        if (pipeline.getAeDelegates() != null && !pipeline.getAeDelegates().isEmpty()) {
            pipeline.getAeDelegates().stream().forEach(itemList::add);
        }
        if (pipeline.getCcDelegates() != null && !pipeline.getCcDelegates().isEmpty())
            pipeline.getCcDelegates().stream().forEach(itemList::add);

        if (itemList.isEmpty())
            textIO.getTextTerminal().executeWithPropertiesPrefix(TerminalPrefixes.WARN, t -> t.print("No components selected yet."));

        String updateAll = "Update all component artifacts to their newest available version";
        String selectManually = "Select the version for each component manually";
        String response = textIO.newStringInputReader().withNumberedPossibleValues(updateAll, selectManually).read("Would you like to update all components at once or select a specific version for each component individually?");

        if (updateAll.equals(response)) {
            for (Description description : itemList) {
                if (description.getMetaDescription().isPear()) {
                    String msg = "Description \"" + description.getName() + "\" is a PEAR and cannot be updated automatically.";
                    textIO.getTextTerminal().print(msg + "\n");
                    log.info(msg);
                    continue;
                }
                try {
                    String newestVersion = MavenConnector.getNewestVersion(description.getMetaDescription().getMavenArtifact());
                    description.getMetaDescription().getMavenArtifact().setVersion(newestVersion);
                    textIO.getTextTerminal().print("Set artifact version of component " + description.getName() + " to " + newestVersion + System.getProperty("line.separator"));
                } catch (MavenException e) {
                    log.error("Could not set the new version to component {}", description.getName(), e);
                }
            }
        } else {
            for (Description description : itemList) {
                new ArtifactVersionMenuItem(description).selectVersion(textIO);
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
