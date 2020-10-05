package de.julielab.jcore.pipeline.builder.cli.menu;

import de.julielab.jcore.pipeline.builder.cli.menu.dialog.IMenuDialog;
import de.julielab.jcore.pipeline.builder.cli.util.PrintLine;
import de.julielab.jcore.pipeline.builder.cli.util.TextIOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.uima.fit.factory.ConfigurationParameterFactory;
import org.apache.uima.resource.ConfigurableDataResourceSpecifier;
import org.apache.uima.resource.ResourceCreationSpecifier;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.ConfigurationParameter;
import org.apache.uima.resource.metadata.NameValuePair;
import org.apache.uima.resource.metadata.ResourceMetaData;
import org.beryx.textio.TextIO;

import java.util.function.Function;
import java.util.stream.Stream;

import static de.julielab.jcore.pipeline.builder.cli.menu.TerminalPrefixes.DEFAULT;
import static de.julielab.jcore.pipeline.builder.cli.menu.TerminalPrefixes.HEADER;

public class ParameterEditingMenuItem implements IMenuDialog {

    private final ResourceSpecifier descriptor;
    private final ConfigurationParameter parameter;

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
        String prompt = "Specify new parameter value. An empty value removes the parameter value assignment:";
        Object selection;
        switch (type) {
            case ConfigurationParameter.TYPE_BOOLEAN:
                selection = textIO.newBooleanInputReader().
                        withPossibleValues(true, false).read("Choose new parameter value:");
                break;
            case ConfigurationParameter.TYPE_FLOAT:
                selection = textIO.newGenericInputReader(new TextIOUtils.EmptyStringParser<>(Double::parseDouble, "float")).read(prompt);
                break;
            case ConfigurationParameter.TYPE_INTEGER:
                selection = textIO.newGenericInputReader(new TextIOUtils.EmptyStringParser<>(Integer::parseInt, "integer")).read(prompt);
                break;
            case ConfigurationParameter.TYPE_STRING:
                selection = textIO.newGenericInputReader(new TextIOUtils.EmptyStringParser<>(Function.identity(), "string")).read(prompt);
                break;
            default:
                throw new IllegalStateException("Unsupported parameter type: " + type);
        }

        if (selection != TextIOUtils.EmptyStringParser.EMPTY_VALUE) {
            ConfigurationParameterFactory.setParameter(descriptor, parameter.getName(), selection);
        } else {
            ResourceMetaData md;
            if (descriptor instanceof ResourceCreationSpecifier) {
                md = ((ResourceCreationSpecifier) descriptor).getMetaData();
            } else if (descriptor instanceof ConfigurableDataResourceSpecifier) {
                md = ((ConfigurableDataResourceSpecifier) descriptor).getMetaData();
            } else {
                throw new IllegalArgumentException("Unhandled descriptor type " + descriptor.getClass().getCanonicalName() + ". This type must be explicitly handled in the code, contact the developer.");
            }
            // Remove the parameter from the settings
            final Stream<NameValuePair> filteredPairs = Stream.of(md.getConfigurationParameterSettings().getParameterSettings()).filter(pair -> !pair.getName().equals(parameter.getName()));
            md.getConfigurationParameterSettings().setParameterSettings(filteredPairs.toArray(NameValuePair[]::new));
        }
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
