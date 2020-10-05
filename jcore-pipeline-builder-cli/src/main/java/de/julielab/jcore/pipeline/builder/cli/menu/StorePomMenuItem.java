package de.julielab.jcore.pipeline.builder.cli.menu;

import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.builder.cli.main.PipelineBuilderCLI;
import de.julielab.utilities.aether.MavenArtifact;
import de.julielab.utilities.aether.MavenException;
import de.julielab.utilities.aether.MavenProjectUtilities;
import org.apache.maven.model.Model;
import org.beryx.textio.TextIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Objects;
import java.util.stream.Stream;

public class StorePomMenuItem implements IMenuItem {
    private final static Logger log = LoggerFactory.getLogger(StorePomMenuItem.class);
    @Override
    public String getName() {
        return "Write a Maven POM for debugging";
    }

    public void execute(JCoReUIMAPipeline pipeline, TextIO textIO) {
        final File pipelinePath = new File(PipelineBuilderCLI.pipelinePath);
        if (!pipelinePath.exists()) {
            textIO.getTextTerminal().executeWithPropertiesPrefix(TerminalPrefixes.ERROR, t -> t.print("Cannot store pom.xml because the pipeline has not been saved yet and thus, no pipeline directory exists." + System.getProperty("line.separator")));
            return;
        }
        final File pomFile = Path.of(PipelineBuilderCLI.pipelinePath, "pom.xml").toFile();
        textIO.getTextTerminal().print("Writing file " + pomFile + ".\n");
        // write the POM template because we need a file to build a Maven model from
        try (final InputStream is = getClass().getResourceAsStream("/pom_template.xml");OutputStream os = new BufferedOutputStream(new FileOutputStream(pomFile))) {
            final ByteBuffer bb = ByteBuffer.allocate(8192);
            int read;
            while ((read = is.read(bb.array())) != -1) {
                bb.position(read);
                bb.flip();
                os.write(bb.array(), 0, bb.limit());
                bb.position(0);
            }
        } catch (IOException e) {
            textIO.getTextTerminal().executeWithPropertiesPrefix(TerminalPrefixes.ERROR, t -> t.print("Could not write the POM due to an I/O exception: " + e.getMessage() + "\n"));
            log.error("IOException while trying to write {}", pomFile, e);
        }

        final Stream.Builder<MavenArtifact> builder = Stream.builder();
        if (pipeline.getCrDescription() != null)
            builder.add(pipeline.getCrDescription().getMetaDescription().getMavenArtifact());
        if (pipeline.getCmDelegates() != null)
            pipeline.getCmDelegates().stream().filter(Objects::nonNull).map(d -> d.getMetaDescription().getMavenArtifact()).forEach(builder::add);
        if (pipeline.getAeDelegates() != null)
            pipeline.getAeDelegates().stream().filter(Objects::nonNull).map(d -> d.getMetaDescription().getMavenArtifact()).forEach(builder::add);
        if (pipeline.getCcDelegates() != null)
            pipeline.getCcDelegates().stream().filter(Objects::nonNull).map(d -> d.getMetaDescription().getMavenArtifact()).forEach(builder::add);
        try {
            final Model model = MavenProjectUtilities.addDependenciesToModel(pomFile, builder.build());
            MavenProjectUtilities.writeModel(pomFile, model);
        } catch (MavenException e) {
            log.error("Exception while adding dependencies to the pipeline Maven model POM file", e);
        } catch (IOException e) {
            log.error("Exception while trying to store Maven model with dependencies to {}", pomFile, e);
        }

    }

    @Override
    public String toString() {
        return getName();
    }
}
