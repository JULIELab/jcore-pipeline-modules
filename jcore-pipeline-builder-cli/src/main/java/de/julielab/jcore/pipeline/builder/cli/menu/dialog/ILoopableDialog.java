package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.cli.menu.BackMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.IMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.QuitMenuItem;
import de.julielab.jcore.pipeline.builder.cli.util.MenuItemExecutionException;
import org.beryx.textio.TextIO;

import java.util.Deque;

public interface ILoopableDialog extends IMenuDialog {
    default void enterInputLoop(TextIO textIO, Deque<String> path) {
        path.add(getName());
        IMenuItem menuItem = executeMenuItem(textIO, path);
        while (!(menuItem instanceof BackMenuItem) && !(menuItem instanceof QuitMenuItem))
            menuItem = executeMenuItem(textIO, path);
        path.removeLast();
    }

    IMenuItem executeMenuItem(TextIO textIO, Deque<String> path);
}
