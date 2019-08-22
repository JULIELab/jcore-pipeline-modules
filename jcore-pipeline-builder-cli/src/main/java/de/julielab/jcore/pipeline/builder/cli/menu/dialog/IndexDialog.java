package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.exceptions.GithubInformationException;
import de.julielab.jcore.pipeline.builder.base.exceptions.PipelineIOException;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.builder.base.main.ComponentMetaInformationService;
import de.julielab.jcore.pipeline.builder.base.main.MetaDescription;
import de.julielab.jcore.pipeline.builder.base.main.Repositories;
import de.julielab.jcore.pipeline.builder.cli.main.PipelineBuilderCLI;
import de.julielab.jcore.pipeline.builder.cli.menu.*;
import de.julielab.jcore.pipeline.builder.cli.util.MenuItemExecutionException;
import de.julielab.jcore.pipeline.builder.cli.util.StatusPrinter;
import de.julielab.jcore.pipeline.builder.cli.util.TextIOUtils;
import org.beryx.textio.TextIO;

import java.util.*;

import static de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants.JcoreMeta.Category;

public class IndexDialog implements ILoopablePipelineManipulationDialog {

    public static final QuitMenuItem QUIT_MENU_ITEM = new QuitMenuItem();
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
        ComponentMetaInformationService.getInstance().getMetaInformation(loadNew).forEach(md -> {
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
        menuItems.add(new RepositoryManagementDialog());
        menuItems.add(new ParentPomSettingDialog());
        menuItems.add(new StorePomMenuItem());
        menuItems.add(QUIT_MENU_ITEM);
    }


    @Override
    public String getName() {
        return "Index";
    }

    @Override
    public IMenuItem executeMenuItem(JCoReUIMAPipeline pipeline, TextIO textIO, Deque<String> path) throws MenuItemExecutionException {
        printPosition(textIO, path);
        StatusPrinter.printPipelineStatus(pipeline, PipelineBuilderCLI.statusVerbosity, textIO);
        if (Repositories.getRepositories().count() == 0)
            TextIOUtils.printLine(TextIOUtils.createPrintLine("There are currently no component repositories active. Navigate to the repository management dialog to add components to build pipelines from.", TerminalPrefixes.WARN), textIO);
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
            } else if (choice instanceof RepositoryManagementDialog) {
                ((RepositoryManagementDialog) choice).enterInputLoop(textIO, path);
                textIO.getTextTerminal().executeWithPropertiesPrefix(TerminalPrefixes.EMPHASIS, t -> t.print("Applying repository changes. It might take a while to fetch remote component meta data." + System.getProperty("line.separator")));
                Repositories.saveRepositoryConfiguration();
                initComponentRepository(false);
                clearTerminal(textIO);
            } else if (choice instanceof  ParentPomSettingDialog) {
                ((ParentPomSettingDialog)choice).execute(pipeline, textIO, path);
            }
            else if (choice instanceof StorePomMenuItem) {
                ((StorePomMenuItem)choice).execute(pipeline, textIO);
            }
        } catch (Exception e) {
            textIO.getTextTerminal().executeWithPropertiesPrefix(TerminalPrefixes.ERROR, t -> t.print("An unexpected exception occurred: " + e.getMessage()));
            e.printStackTrace();
        }
        return choice;
    }
}
