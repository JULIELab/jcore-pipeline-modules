package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import com.google.common.collect.Multiset;
import de.julielab.jcore.pipeline.builder.base.exceptions.PipelineIOException;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.builder.cli.main.PipelineBuilderCLI;
import org.beryx.textio.TextIO;

import java.io.File;
import java.util.Comparator;
import java.util.Deque;
import java.util.Optional;

import static de.julielab.jcore.pipeline.builder.cli.menu.TerminalPrefixes.ERROR;

public class SavePipelineDialog implements IMenuDialog {
    @Override
    public String getName() {
        return "Save Pipeline";
    }

    public void savePipeline(JCoReUIMAPipeline pipeline, TextIO textIO, Deque<String> path) throws PipelineIOException {
        path.add(getName());
        printPosition(textIO, path);

        // Check if the component names are unique because otherwise, saving the pipeline will make it invalid
        final Multiset<String> existingDescriptorNames = pipeline.getExistingDescriptorNames();
        final Optional<Integer> max = existingDescriptorNames.stream().map(existingDescriptorNames::count).max(Comparator.reverseOrder());
        if (max.isPresent() && max.get() > 1) {
            textIO.getTextTerminal().executeWithPropertiesPrefix(ERROR, t -> t.println("The pipeline cannot be saved because there are multiple components with the same name. Saving the pipeline now would render it invalid and unable to load again."));
        } else {
            String destination = textIO.newStringInputReader().withDefaultValue(PipelineBuilderCLI.pipelinePath).read("Enter the directory to save the pipeline to.");
            PipelineBuilderCLI.pipelinePath = destination;
            File destinationFile = new File(destination);
            boolean store = true;
            boolean storeLibraries = PipelineBuilderCLI.dependenciesHaveChanged;
            if (destinationFile.exists()) {
                Boolean overwrite = textIO.newBooleanInputReader()
                        .withDefaultValue(false)
                        .withFalseInput("N")
                        .withTrueInput("Y")
                        .read("The path " + destinationFile.getAbsolutePath() + " exists. Do you wish to store to " +
                                "this directory anyway?");
                if (overwrite && !PipelineBuilderCLI.dependenciesHaveChanged) {
                    Boolean clear = textIO.newBooleanInputReader().
                            withDefaultValue(false).
                            withFalseInput("N").
                            withTrueInput("Y").
                            read("Do you want to force to clear and (re-) populate the " + JCoReUIMAPipeline.DIR_LIB + " directory? This can be necessary in case the pipeline builder fails to recognize that dependencies have changed.");
                    if (clear) {
                        storeLibraries = true;
                    }
                }
                if (!overwrite) {
                    store = false;
                    textIO.getTextTerminal().println("Aborting.");
                }
            }
            if (store) {
                textIO.getTextTerminal().println("Storing pipeline. It may take a while to gather all transitive " +
                        "dependencies, please wait...");
                pipeline.store(destinationFile, storeLibraries);
                if (storeLibraries)
                    PipelineBuilderCLI.dependenciesHaveChanged = false;
                textIO.getTextTerminal().println("Saved pipeline to " + destinationFile.getAbsolutePath());
            }
        }
        path.removeLast();
    }

    @Override
    public String toString() {
        return getName();
    }
}
