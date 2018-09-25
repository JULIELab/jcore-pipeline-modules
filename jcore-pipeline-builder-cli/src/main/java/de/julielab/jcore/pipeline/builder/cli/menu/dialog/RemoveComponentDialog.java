package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.builder.cli.menu.BackMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.ComponentSelectionMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.IMenuItem;
import de.julielab.jcore.pipeline.builder.cli.util.MenuItemExecutionException;
import org.beryx.textio.TextIO;

import java.util.Deque;

public class RemoveComponentDialog extends AbstractComponentSelectionDialog {
    @Override
    public IMenuItem executeMenuItem(JCoReUIMAPipeline pipeline, TextIO textIO, Deque<String> path) throws MenuItemExecutionException {
        IMenuItem choice = super.executeMenuItem(pipeline, textIO, path);
        if (!(choice instanceof BackMenuItem)) {
            ComponentSelectionMenuItem item = (ComponentSelectionMenuItem) choice;
            pipeline.removeComponent(item.getDescription());
        }
        return choice;
    }

    @Override
    public String getName() {
        return "Remove Components";
    }
}
