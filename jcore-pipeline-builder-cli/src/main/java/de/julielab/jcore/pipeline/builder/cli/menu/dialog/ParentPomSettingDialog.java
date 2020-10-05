package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.builder.cli.menu.BackMenuItem;
import de.julielab.utilities.aether.MavenArtifact;
import org.beryx.textio.TextIO;

import java.io.File;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

public class ParentPomSettingDialog implements IMenuDialog {

    public void execute(JCoReUIMAPipeline pipeline, TextIO textIO, Deque<String> path) {
        clearTerminal(textIO);
        path.add(getName());
        printPosition(textIO, path);
        String linesep = System.getProperty("line.separator");
        textIO.getTextTerminal().print("Setting a pipeline parent POM is optional and typically only required if " +
                "some of the employed components exhibit a library version conflict. In this case, a parent POM " +
                "may specify the version to use in the dependencyManagement section." + linesep);
        String currentPom = pipeline.getParentPom() != null ? pipeline.getParentPom().getGroupId()  + ":" + pipeline.getParentPom().getArtifactId() + ":" + pipeline.getParentPom().getVersion() + "(file: " + pipeline.getParentPom().getFile().getAbsolutePath() + ")" : "<none>";
        textIO.getTextTerminal().print("Current parent POM: " + currentPom + linesep);
        List<Object> pomSourceOptions = Arrays.asList("Specify parent through maven coordinates", "Specify parent POM file",BackMenuItem.get());
        Object sourceChoice = textIO.newGenericInputReader(null)
                .withNumberedPossibleValues(pomSourceOptions).withDefaultValue(BackMenuItem.get())
                .read("Choose an option:");
        if (pomSourceOptions.indexOf(sourceChoice) == 0) {
            String groupId = textIO.newStringInputReader().read("Specify the groupId:");
            String artifactId = textIO.newStringInputReader().read("Specify the artifactId:");
            String version = textIO.newStringInputReader().read("Specify the version:");
            final MavenArtifact artifact = new MavenArtifact(groupId, artifactId, version);
            artifact.setPackaging("pom");
            pipeline.setParentPom(artifact);
        } else if (pomSourceOptions.indexOf(sourceChoice) == 1) {
            final String parentPath = textIO.newStringInputReader().read("Specify the path to the POM file:");
            final MavenArtifact artifact = new MavenArtifact();
            artifact.setFile(new File(parentPath));
            pipeline.setParentPom(artifact);
        }
        path.pop();
    }

    @Override
    public String getName() {
        return "Set pipeline components parent POM";
    }

    @Override
    public String toString() {
        return getName();
    }
}
