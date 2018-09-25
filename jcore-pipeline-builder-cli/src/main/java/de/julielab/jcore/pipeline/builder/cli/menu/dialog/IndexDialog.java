package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.exceptions.GithubInformationException;
import de.julielab.jcore.pipeline.builder.base.exceptions.PipelineIOException;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.builder.base.main.JcoreGithubInformationService;
import de.julielab.jcore.pipeline.builder.base.main.MetaDescription;
import de.julielab.jcore.pipeline.builder.cli.menu.IMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.QuitMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.RefreshComponentRepositoryMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.TerminalPrefixes;
import de.julielab.jcore.pipeline.builder.cli.util.MenuItemExecutionException;
import de.julielab.jcore.pipeline.builder.cli.util.StatusPrinter;
import org.beryx.textio.TextIO;

import java.util.*;

import static de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants.JcoreMeta.Category;

public class IndexDialog implements ILoopablePipelineManipulationDialog {

    private Map<Category, List<MetaDescription>> categoryMap;
    private List<IMenuItem> menuItems;

    public IndexDialog() throws GithubInformationException {
        initComponentRepository(false);
    }

    private void initComponentRepository(boolean loadNew) throws GithubInformationException {
        categoryMap = new HashMap<>();
        // Groups the meta descriptions by their categories. We cannot just use the groupBy Java8 collector because
        // we have multiple categories. In Java9 there is the stream group by which should work here:
        // http://www.baeldung.com/java9-stream-collectors
        JcoreGithubInformationService.getInstance().getMetaInformation(loadNew).forEach(md -> {
            for (Category category : md.getCategories()) {
                categoryMap.compute(category, (k, v) -> {
                    List<MetaDescription> ret = v;
                    if (ret == null)
                        ret = new ArrayList<>();
                    ret.add(md);
                    return ret;
                });
            }
        });
        menuItems = new ArrayList<>();
        menuItems.add(new AddComponentDialog(categoryMap, Category.reader));
        menuItems.add(new AddComponentDialog(categoryMap, Category.multiplier));
        menuItems.add(new AddComponentDialog(categoryMap, Category.ae));
        menuItems.add(new AddComponentDialog(categoryMap, Category.consumer));
        menuItems.add(new ConfigurePipelineDialog());
        menuItems.add(new SavePipelineDialog());
        menuItems.add(new LoadPipelineDialog());
        menuItems.add(new RefreshComponentRepositoryMenuItem());
        menuItems.add(new QuitMenuItem());
    }


    @Override
    public String getName() {
        return "Index";
    }

    @Override
    public IMenuItem executeMenuItem(JCoReUIMAPipeline pipeline, TextIO textIO, Deque<String> path) throws MenuItemExecutionException {
        printPosition(textIO, path);
        StatusPrinter.printPipelineStatus(pipeline, textIO);
        IMenuItem choice = textIO.<IMenuItem>newGenericInputReader(null)
                .withNumberedPossibleValues(menuItems)
                .read("\nAdd or remove components from your pipeline.");
        clearTerminal(textIO);
        try {
            if (choice instanceof ILoopablePipelineManipulationDialog)
                ((ILoopablePipelineManipulationDialog) choice).enterInputLoop(pipeline, textIO, path);
            else if (choice instanceof SavePipelineDialog) {
                try {
                    ((SavePipelineDialog) choice).savePipeline(pipeline, textIO, path);
                } catch (PipelineIOException e) {
                    e.printStackTrace();
                    textIO.getTextTerminal().executeWithPropertiesPrefix(TerminalPrefixes.ERROR,
                            t -> t.print("Saving the pipeline failed: " + e.getMessage()));
                }
            } else if (choice instanceof LoadPipelineDialog) {
                try {
                    ((LoadPipelineDialog) choice).loadPipeline(pipeline, textIO, path);
                } catch (PipelineIOException e) {
                    e.printStackTrace();
                    textIO.getTextTerminal().executeWithPropertiesPrefix(TerminalPrefixes.ERROR,
                            t -> t.print("Loading the pipeline failed: " + e.getMessage()));
                }
            } else if (choice instanceof RefreshComponentRepositoryMenuItem) {
                try {
                    textIO.getTextTerminal().print("Refreshing component repository...");
                    initComponentRepository(true);
                    textIO.getTextTerminal().println("Done.");
                } catch (GithubInformationException e) {
                    throw new MenuItemExecutionException(e);
                }
            }
        } catch (Exception e) {
            textIO.getTextTerminal().executeWithPropertiesPrefix(TerminalPrefixes.ERROR, t -> t.print("An unexpected exception occurred: " + e.getMessage()));
            e.printStackTrace();
        }
        return choice;
    }
}