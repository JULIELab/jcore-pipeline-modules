package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.cli.main.PipelineBuilderCLI;
import de.julielab.jcore.pipeline.builder.cli.util.StatusPrinter;
import org.beryx.textio.TextIO;

public class SpecifyStatusVerbosityDialog implements IMenuDialog {

    public void chooseVerbosity(TextIO textIO) {
        final StatusPrinter.Verbosity newVerbosity = textIO.<StatusPrinter.Verbosity>newGenericInputReader(null)
                .withNumberedPossibleValues(StatusPrinter.Verbosity.values())
                .withDefaultValue(PipelineBuilderCLI.statusVerbosity)
                .read("Specify the pipeline overview verbosity level:");
        PipelineBuilderCLI.statusVerbosity = newVerbosity;
    }

    @Override
    public String getName() {
        return "Specify Pipeline Status Verbosity";
    }

    @Override
    public String toString() {
        return getName();
    }
}
