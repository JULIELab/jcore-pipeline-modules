package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants.JcoreMeta.Category;
import de.julielab.jcore.pipeline.builder.base.exceptions.DescriptorLoadingException;
import de.julielab.jcore.pipeline.builder.base.main.Description;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.builder.base.main.MetaDescription;
import de.julielab.jcore.pipeline.builder.cli.main.PipelineBuilderCLI;
import de.julielab.jcore.pipeline.builder.cli.menu.*;
import de.julielab.jcore.pipeline.builder.cli.util.StatusPrinter;
import org.beryx.textio.TextIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static de.julielab.jcore.pipeline.builder.cli.menu.TerminalPrefixes.ERROR;

public class AddComponentDialog implements ILoopablePipelineManipulationDialog {
    private final static Logger log = LoggerFactory.getLogger(AddComponentDialog.class);
    private final List<MetaDescription> aeDescriptions;
    private final Category category;
    private MenuItemList<IMenuItem> menuItemList;

    public AddComponentDialog(Map<Category, List<MetaDescription>> categoryMap, Category category) {
        this.aeDescriptions = categoryMap.getOrDefault(category, Collections.emptyList());
        this.category = category;
        menuItemList = new MenuItemList();
        for (MetaDescription aeDesc : aeDescriptions)
            menuItemList.add(new EditMenuItem(aeDesc, category));
        Collections.sort(menuItemList);
        menuItemList.add(new BackMenuItem());
    }

    @Override
    public IMenuItem executeMenuItem(JCoReUIMAPipeline pipeline, TextIO textIO, Deque<String> path) {
        printPosition(textIO, path);
        StatusPrinter.printPipelineStatus(pipeline, textIO);
        IMenuItem choice = textIO.<IMenuItem>newGenericInputReader(null)
                .withNumberedPossibleValues(menuItemList)
                .read("\nChoose a component.");
        boolean errorPrinted = false;
        try {
            if (choice instanceof EditMenuItem) {
                EditMenuItem addItem = (EditMenuItem) choice;
                MetaDescription description = addItem.getDescription();
                textIO.getTextTerminal().print("Loading component..." + System.getProperty("line.separator"));
                Collection<Description> jCoReDescriptions;
                try {
                    jCoReDescriptions = description.getJCoReDescriptions();
                } catch (DescriptorLoadingException e) {
                    clearTerminal(textIO);
                    textIO.getTextTerminal().executeWithPropertiesPrefix(ERROR, t -> t.println("Could not load the " +
                            "component due to an exception: " + e.getMessage()));
                    errorPrinted = true;
                    throw e;
                }
                clearTerminal(textIO);
                if (jCoReDescriptions.size() > 1) {
                    new SingleDescriptionSelectionDialog(description, d -> d.getCategory() == category).chooseDescription(textIO, path);
                } else {
                    clearTerminal(textIO);
                }
                Description jCoReDescription = description.getChosenDescriptor().clone();
                if (jCoReDescription != null) {
                    try {
                        switch (category) {
                            case reader:
                                pipeline.setCrDescription(jCoReDescription);
                                break;
                            case multiplier:
                                pipeline.addCasMultiplier(jCoReDescription);
                                break;
                            case ae:
                                pipeline.addDelegateAe(jCoReDescription);
                                break;
                            case consumer:
                                pipeline.addCcDesc(jCoReDescription);
                                break;
                            default:
                                textIO.getTextTerminal().executeWithPropertiesPrefix(ERROR, t ->
                                        t.println("Could not set the component because it belongs to the unsupported " +
                                                "component category \"" + category.name() + "\"."));
                                errorPrinted = true;
                        }
                        PipelineBuilderCLI.dependenciesHaveChanged = true;
                        clearTerminal(textIO);
                    } catch (Exception e) {
                        textIO.getTextTerminal().executeWithPropertiesPrefix(
                                TerminalPrefixes.ERROR, t -> t.print("Could not add the analysis engine: " + e.getMessage()));
                        errorPrinted = true;
                        e.printStackTrace();
                    }
                } else {
                    textIO.getTextTerminal().executeWithPropertiesPrefix(TerminalPrefixes.ERROR,
                            t -> t.println("Could not add the analysis engine because there is no descriptor."));
                    errorPrinted = true;
                }
            }
        } catch (DescriptorLoadingException e) {
            log.debug("Could not load descriptors", e);
        } catch (CloneNotSupportedException e) {
            log.error("Could not clone the description", e);
        }
        IMenuItem iMenuItem = category == Category.reader ? new BackMenuItem() : choice;
        if (iMenuItem instanceof BackMenuItem && !errorPrinted)
            clearTerminal(textIO);
        if (choice instanceof EditMenuItem) {
            textIO.getTextTerminal().executeWithPropertiesPrefix(TerminalPrefixes.EMPHASIS, t -> t.println("Added component:"));
            StatusPrinter.printComponentMetaData(((EditMenuItem) choice).getDescription(), textIO);
            textIO.getTextTerminal().println(System.getProperty("line.separator"));
        }
        return iMenuItem;
    }

    @Override
    public String getName() {
        switch (category) {
            case reader:
                return "Collection Reader";
            case multiplier:
                return "CAS Multiplier";
            case ae:
                return "Analysis Engine";
            case consumer:
                return "CAS Consumer";
            default:
                return "<unsupported component category>";
        }
    }

    @Override
    public String toString() {
        return getName();
    }

}
