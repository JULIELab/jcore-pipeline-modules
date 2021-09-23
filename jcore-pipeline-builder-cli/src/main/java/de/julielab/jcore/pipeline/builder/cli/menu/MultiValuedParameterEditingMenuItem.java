package de.julielab.jcore.pipeline.builder.cli.menu;

import de.julielab.jcore.pipeline.builder.cli.menu.dialog.IMenuDialog;
import de.julielab.jcore.pipeline.builder.cli.util.PrintLine;
import de.julielab.jcore.pipeline.builder.cli.util.TextIOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.uima.resource.ConfigurableDataResourceSpecifier;
import org.apache.uima.resource.ResourceCreationSpecifier;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.ConfigurationParameter;
import org.apache.uima.resource.metadata.ResourceMetaData;
import org.beryx.textio.TextIO;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static de.julielab.jcore.pipeline.builder.cli.menu.TerminalPrefixes.DEFAULT;
import static de.julielab.jcore.pipeline.builder.cli.menu.TerminalPrefixes.HEADER;

public class MultiValuedParameterEditingMenuItem implements IMenuDialog {

    private final ResourceSpecifier descriptor;
    private final ConfigurationParameter parameter;

    public MultiValuedParameterEditingMenuItem(ResourceSpecifier description, ConfigurationParameter parameter) {
        this.descriptor = description;
        this.parameter = parameter;
    }

    public ConfigurationParameter getParameter() {
        return parameter;
    }

    public void setParameterValue(TextIO textIO) {

        Stream<PrintLine> lineStream = Stream.empty();
        if (!StringUtils.isBlank(parameter.getDescription()))
            lineStream = Stream.concat(lineStream, Stream.of(
                    TextIOUtils.createPrintLine("The parameter description is:",
                            HEADER),
                    TextIOUtils.createPrintLine(parameter.getDescription(), DEFAULT)
            ));
        else
            lineStream = Stream.concat(lineStream, Stream.of(TextIOUtils.createPrintLine("No parameter description is available.", DEFAULT)));
        TextIOUtils.printLines(lineStream, textIO);

        ResourceMetaData md;
        if (descriptor instanceof ResourceCreationSpecifier) {
            md = ((ResourceCreationSpecifier) descriptor).getMetaData();
        } else if (descriptor instanceof ConfigurableDataResourceSpecifier) {
            md = ((ConfigurableDataResourceSpecifier) descriptor).getMetaData();
        } else {
            throw new IllegalArgumentException("Unhandled descriptor type " + descriptor.getClass().getCanonicalName() + ". This type must be explicitly handled in the code, contact the developer.");
        }

        Object[] array = (Object[]) md.getConfigurationParameterSettings().getParameterValue(parameter.getName());
        if (array == null)
            array = new Object[0];
        printCurrentValues(array, textIO);

        String response;
        do {
            response = textIO.<String>newGenericInputReader(null).withNumberedPossibleValues(
                            "Add element",
                            "Remove element",
                            "Back").
                    withDefaultValue("Back").
                    read("Select an action:");
            switch (response) {
                case "Add element":
                    while ((array = addValue(array, textIO)) != null) {
                        md.getConfigurationParameterSettings().setParameterValue(parameter.getName(), array);
                        printCurrentValues(array, textIO);
                    }
                    // The loop above terminates when the input value is null which sets the local 'array' variable
                    // to null. But the do-while continues to work on 'array' in the next round, if more user
                    // wishes to make more changes. Thus, we need to set the complete current value to the variable.
                    array = (Object[]) md.getConfigurationParameterSettings().getParameterValue(parameter.getName());
                    break;

                case "Remove element":
                    Integer toRemove;
                    do {
                        toRemove = textIO.newIntInputReader().withMinVal(0).withMaxVal(array.length).withDefaultValue(0).read("Select an item to remove or 0 for none:");
                        if (toRemove > 0) {
                            List<Object> objList = new ArrayList<>(Arrays.asList(array));
                            objList.remove(toRemove - 1);
                            array = objList.toArray(new Object[0]);
                            md.getConfigurationParameterSettings().setParameterValue(parameter.getName(), array);
                            printCurrentValues(array, textIO);
                        }
                    } while (toRemove > 0);
                    break;
            }
        } while (!response.equals("Back"));
    }

    private void printCurrentValues(Object[] array, TextIO textIO) {
        if (array.length > 0) {
            String ls = System.getProperty("line.separator");
            StringBuilder sb = new StringBuilder();
            sb.append("Current parameter values:");
            sb.append(ls);
            for (int i = 0; i < array.length; ++i) {
                Object o = array[i];
                sb.append(i + 1).append(": ");
                sb.append(o.toString());
                sb.append(ls);
            }
            textIO.getTextTerminal().print(sb.toString());
        } else {
            textIO.getTextTerminal().println("The parameter has currently no values.");
        }
    }

    private Object[] addValue(Object[] array, TextIO textIO) {
        String prompt = "Specify new parameter value. Enter empty value to end input:";
        Object[] newArray;
        int length = array != null ? array.length + 1 : 1;
        String input = textIO.newStringInputReader().withMinLength(0).read(prompt);
        if (input.isEmpty())
            return null;
        Object newElement;
        switch (parameter.getType()) {
            case ConfigurationParameter.TYPE_BOOLEAN:
                newArray = new Boolean[length];
                newElement = Boolean.parseBoolean(input);
                break;
            case ConfigurationParameter.TYPE_FLOAT:
                newArray = new Double[length];
                newElement = Double.parseDouble(input);
                break;
            case ConfigurationParameter.TYPE_INTEGER:
                newArray = new Integer[length];
                newElement = Integer.parseInt(input);
                break;
            case ConfigurationParameter.TYPE_STRING:
                newArray = new String[length];
                newElement = input;
                break;
            default:
                throw new IllegalStateException("Unsupported parameter type: " + parameter.getType());
        }
        System.arraycopy(array, 0, newArray, 0, array.length);
        newArray[newArray.length - 1] = newElement;
        return newArray;
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
