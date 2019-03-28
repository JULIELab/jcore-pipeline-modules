package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.builder.cli.menu.BackMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.IMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.QuitMenuItem;
import de.julielab.jcore.pipeline.builder.cli.util.MenuItemExecutionException;
import org.beryx.textio.TextIO;

import java.util.Deque;


public interface ILoopablePipelineManipulationDialog extends IMenuDialog {
    IMenuItem executeMenuItem(JCoReUIMAPipeline pipeline, TextIO textIO, Deque<String> path) throws MenuItemExecutionException;

    default void enterInputLoop(JCoReUIMAPipeline pipeline, TextIO textIO, Deque<String> path) throws MenuItemExecutionException {
        path.add(getName());
        IMenuItem menuItem = executeMenuItem(pipeline, textIO, path);
        while (!(menuItem instanceof BackMenuItem) && !(menuItem instanceof QuitMenuItem))
            menuItem = executeMenuItem(pipeline, textIO, path);
        path.removeLast();
    }
}
