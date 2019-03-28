package de.julielab.jcore.pipeline.builder.cli.util;

import org.beryx.textio.InputReader;
import org.beryx.textio.TextIO;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static de.julielab.jcore.pipeline.builder.cli.menu.TerminalPrefixes.DEFAULT;

public class TextIOUtils {
    /**
     * A textIO parser to use with {@link TextIO#newGenericInputReader(Function)}. It will return <tt>null</tt> if
     * an empty value is input (i.e. the user did immediately hit return) or the value delivered by a converter
     * otherwise.
     * @param <T>
     */
    public static class EmptyStringParser<T> implements Function<String, InputReader.ParseResult<Object>> {
        public static final Object EMPTY_VALUE = new Object();
        private Function<String, T> converter;
        private String typeName;

        public EmptyStringParser(Function<String, T> converter, String typeName) {
            this.converter = converter;
            this.typeName = typeName;
        }

        @Override
        public InputReader.ParseResult<Object> apply(String s) {
            if (s.isEmpty())
                return new InputReader.ParseResult<>(EMPTY_VALUE);
            try {
                T convertedValue = converter.apply(s);
                return new InputReader.ParseResult<>(convertedValue);
            } catch (NumberFormatException e) {
                return new InputReader.ParseResult<>(EMPTY_VALUE, "Enter a " + typeName + " value.");
            }
        }
    }

    public static void printLines(Stream<PrintLine> records, TextIO textIO) {
        String LS = System.getProperty("line.separator");
        records.forEach(line -> {
            line.forEach(record -> textIO.getTextTerminal()
                    .executeWithPropertiesPrefix(record.getPrefix(),
                            t -> t.print(record != null && record.getText() != null ? record.getText() : "<null>")));
            // To end the line and reset the JLine terminal
            textIO.getTextTerminal().executeWithPropertiesPrefix(DEFAULT, t -> t.print(LS));
        });
    }

    /**
     * Converts an array of text and prefix strings into {@link PrintElement} objects.
     * Note that the returned list is actually of type {@link PrintLine} and thus can safely be casted to this type if
     * required.
     *
     * @param input An array of text and TextIO prefix identifiers, always as pairs in this order.
     * @return The converted print element list.
     */
    public static List<PrintElement> createPrintElements(String... input) {
        PrintLine elements = new PrintLine();
        for (int i = 0; i < input.length; i++) {
            String s = input[i];
            if (i % 2 == 1) {
                elements.add(new PrintElement(input[i - 1], input[i]));
            }
        }
        return elements;
    }

    /**
     * Converts an array of text and prefix strings into {@link PrintElement} objects and does the exact same thing
     * as {@link #createPrintElements(String...)} except it returns the result as a {@link PrintLine}.
     * This is only for convenience to distinguish about arbitrary lists of print elements and lists that should
     * represent a line of output.
     *
     * @param input An array of text and TextIO prefix identifiers, always as pairs in this order.
     * @return The converted print element list.
     */
    public static PrintLine createPrintLine(String... input) {
        return (PrintLine) createPrintElements(input);
    }

    public static PrintElement createPrintElement(String text, String prefix) {
        return new PrintElement(text, prefix);
    }

    /**
     * Prints the passed elements according to their respective prefix.
     *
     * @param printElements The elements to print.
     * @param textIO        The textIO instance of output.
     */
    public static void printElements(Stream<PrintElement> printElements, TextIO textIO) {
        printElements.forEach(e -> textIO.getTextTerminal().
                executeWithPropertiesPrefix(e.getPrefix(), t -> t.print(e.getText())));
    }

}
