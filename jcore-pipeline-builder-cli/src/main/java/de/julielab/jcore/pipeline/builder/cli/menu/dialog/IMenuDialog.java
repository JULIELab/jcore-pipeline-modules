package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.cli.menu.IMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.TerminalPrefixes;
import org.beryx.textio.TextIO;
import org.beryx.textio.TextTerminal;
import org.beryx.textio.jline.JLineTextTerminal;
import org.beryx.textio.swing.SwingTextTerminal;

import java.util.Deque;
import java.util.stream.Collectors;

public interface IMenuDialog extends IMenuItem {
    default void printPosition(TextIO textIO, Deque<String> path) {
        String LS = System.getProperty("line.separator");
        String pathString = path.stream().collect(Collectors.joining("/"));
        textIO.getTextTerminal().executeWithPropertiesPrefix(TerminalPrefixes.PATH, t -> t.print("You are here: " + pathString));
        // For the SwingTextTerminal: It does not reset its properties after the above line
        textIO.getTextTerminal().executeWithPropertiesPrefix(TerminalPrefixes.DEFAULT ,t -> t.print(LS));
    }

    default void clearTerminal(TextIO textIO) {
        TextTerminal<?> terminal = textIO.getTextTerminal();
        Class<? extends TextTerminal> terminalClass = terminal.getClass();
        if(terminalClass.equals(SwingTextTerminal.class)){
            terminal.resetToBookmark("clearscreen");
            terminal.setBookmark("clearscreen");
            ((SwingTextTerminal)terminal).setPaneBackgroundColor("white");
        } else if (terminalClass.equals(JLineTextTerminal.class)){
            // Taken from https://stackoverflow.com/a/32295974/1314955
            terminal.print("\033[H\033[2J");
        }
    }

}
