package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.java.utilities.classpath.JarLoader;
import de.julielab.jcore.pipeline.builder.base.exceptions.PipelineIOException;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.builder.cli.main.PipelineBuilderCLI;
import org.beryx.textio.TextIO;

import java.io.File;
import java.util.Deque;
import java.util.stream.Stream;

public class LoadPipelineDialog implements IMenuDialog {

    /**
     * @param pipeline The empty pipeline object to load values into that have been stored to disc.
     */
    public void loadPipeline(JCoReUIMAPipeline pipeline, TextIO textIO, Deque<String> path) throws PipelineIOException {
        path.add(getName());
        printPosition(textIO, path);
        String source = textIO.newStringInputReader().withDefaultValue(PipelineBuilderCLI.pipelinePath).read("Enter the directory to load the pipeline from.");
        PipelineBuilderCLI.pipelinePath = source;
        File loadDirectory = new File(source);
        pipeline.clear();
        pipeline.setLoadDirectory(loadDirectory);
        textIO.getTextTerminal().println("Loading pipeline from " + loadDirectory.getAbsolutePath());
        pipeline.load(true);
        path.removeLast();
    }


    @Override
    public String getName() {
        return "Load Pipeline";
    }

    @Override
    public String toString() {
        return getName();
    }
}
