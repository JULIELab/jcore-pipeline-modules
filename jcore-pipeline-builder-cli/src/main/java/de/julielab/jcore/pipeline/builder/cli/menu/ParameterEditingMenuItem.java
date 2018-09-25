package de.julielab.jcore.pipeline.builder.cli.menu;

import de.julielab.jcore.pipeline.builder.cli.menu.dialog.IMenuDialog;
import de.julielab.jcore.pipeline.builder.cli.util.PrintLine;
import de.julielab.jcore.pipeline.builder.cli.util.TextIOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.uima.fit.factory.ConfigurationParameterFactory;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.ConfigurationParameter;
import org.beryx.textio.TextIO;

import java.util.stream.Stream;

import static de.julielab.jcore.pipeline.builder.cli.menu.TerminalPrefixes.DEFAULT;
import static de.julielab.jcore.pipeline.builder.cli.menu.TerminalPrefixes.HEADER;

public class ParameterEditingMenuItem implements IMenuDialog {

    private ResourceSpecifier descriptor;
    private ConfigurationParameter parameter;

    public ParameterEditingMenuItem(ResourceSpecifier descriptor, ConfigurationParameter parameter) {
        this.descriptor = descriptor;
        this.parameter = parameter;
    }

    public ConfigurationParameter getParameter() {
        return parameter;
    }

    public void setParameterValue(TextIO textIO) {
        String type = parameter.getType();
        Stream<PrintLine> lineStream = Stream.of(
                TextIOUtils.createPrintLine("Please specify a new value for the parameter \"" + parameter.getName() + "\".",
                        HEADER));
        if (!StringUtils.isBlank(parameter.getDescription()))
            lineStream = Stream.concat(lineStream, Stream.of(
                    TextIOUtils.createPrintLine("The parameter descriptor is:",
                            HEADER),
                    TextIOUtils.createPrintLine(parameter.getDescription(), DEFAULT)
            ));
        else
            lineStream = Stream.concat(lineStream, Stream.of(TextIOUtils.createPrintLine("No parameter description is available.", DEFAULT)));
        TextIOUtils.printLines(lineStream, textIO);
        String prompt = "Specify new parameter value:";
        Object selection;
        switch (type) {
            case ConfigurationParameter.TYPE_BOOLEAN:
                selection = textIO.newBooleanInputReader().
                        withPossibleValues(true, false).read("Choose new parameter value:");
                break;
            case ConfigurationParameter.TYPE_FLOAT:
                selection = textIO.newDoubleInputReader().read(prompt);
                break;
            case ConfigurationParameter.TYPE_INTEGER:
                selection = textIO.newIntInputReader().read(prompt);
                break;
            case ConfigurationParameter.TYPE_STRING:
                selection = textIO.newStringInputReader().read(prompt);
                break;
            default:
                throw new IllegalStateException("Unsupported parameter type: " + type);
        }

        ConfigurationParameterFactory.setParameter(descriptor, parameter.getName(), selection);

    }

    @Override
    public String getName() {
        return parameter.getName();
    }

    @Override
    public String toString() {
        return getName();
    }
}
