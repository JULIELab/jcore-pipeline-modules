package de.julielab.jcore.pipeline.builder.cli.main;

import de.julielab.java.utilities.prerequisites.PrerequisiteChecker;
import de.julielab.jcore.pipeline.builder.base.exceptions.GithubInformationException;
import de.julielab.jcore.pipeline.builder.base.exceptions.PipelineIOException;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.builder.base.main.Repositories;
import de.julielab.jcore.pipeline.builder.cli.menu.TerminalPrefixes;
import de.julielab.jcore.pipeline.builder.cli.menu.dialog.IndexDialog;
import de.julielab.jcore.pipeline.builder.cli.menu.dialog.RepositoryAddDialog;
import de.julielab.jcore.pipeline.builder.cli.util.MenuItemExecutionException;
import org.beryx.textio.TextIO;
import org.beryx.textio.TextIoFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayDeque;

import static de.julielab.jcore.pipeline.builder.cli.menu.TerminalPrefixes.DEFAULT;
import static de.julielab.jcore.pipeline.builder.cli.menu.TerminalPrefixes.WELCOME;

public class PipelineBuilderCLI {
    private final static Logger log = LoggerFactory.getLogger(PipelineBuilderCLI.class);
    /**
     * This path is used by loading and saving dialogs as the default value for the pipeline path. The user entered
     * value is always set to this path.
     */
    public static String pipelinePath = "pipeline";
    /**
     * This value keeps track for the case that the dependencies of the pipeline have changed. If so, they
     * need to be resolved and stored into the library directory upon pipeline storage.
     * <p>
     * This field is set to <tt>true</tt> if components are added or removed from the pipeline or if component versions have been set.
     */
    public static boolean dependenciesHaveChanged = false;

    public static void main(String args[]) {
        System.setProperty(PrerequisiteChecker.PREREQUISITE_CHECKS_ENABLED, "true");

        TextIO textIO = null;
        JCoReUIMAPipeline pipeline = new JCoReUIMAPipeline();
        try {
            if (args.length > 0) {
                pipeline.setLoadDirectory(new File(args[0]));
                pipeline.load(true);
                pipelinePath = args[0];
            }
            textIO = TextIoFactory.getTextIO();
            IndexDialog indexDialog = new IndexDialog();
            indexDialog.clearTerminal(textIO);
            textIO.getTextTerminal().executeWithPropertiesPrefix(WELCOME,
                    t -> t.println("Welcome to the JCoRe Pipeline Builder"));
            textIO.getTextTerminal().executeWithPropertiesPrefix(DEFAULT, t ->
                    t.println("This tool is supposed to help with the creation of UIMA workflows using " +
                            "components from the JCoRe repository. While the tool tries to be as " +
                            "transparent and helpful as possible, a basic understanding of UIMA, " +
                            "JCoRe and the individual JCoRe components is necessary. For help and " +
                            "pointers to the adequate documentation, please refer to the README of " +
                            "the pipeline modules at https://github.com/JULIELab/jcore-pipeline-modules"));
            if (Repositories.loadActiveRepositories().isEmpty()) {
                new RepositoryAddDialog().enterInputLoop(textIO, new ArrayDeque<>());
            }
            indexDialog.enterInputLoop(pipeline, textIO, new ArrayDeque<>());
        } catch (GithubInformationException | MenuItemExecutionException e) {
            if (e instanceof GithubInformationException || e.getCause() instanceof GithubInformationException) {
                log.debug("Error when loading component list", e);
                String ls = System.getProperty("line.separator");
                textIO.getTextTerminal().executeWithPropertiesPrefix(TerminalPrefixes.ERROR, t ->
                        t.print("Could not load JCoRe component list, the program is aborted."
                                + ls + "The error is: " + e.getMessage() + ls));
            } else if (e instanceof MenuItemExecutionException)
                log.debug("Error when running menu", e.getCause());
            else
                log.error("Error occurred while running application menu", e);
        } catch (PipelineIOException e) {
            log.error("Could not load the pipeline at {}: ", pipeline.getLoadDirectory(), e);
        } finally {
            if (textIO != null) {
                // Fixes the issue on the console that after the program terminates, the font color is set to the
                // prompt color used within this program
                textIO.getTextTerminal().getProperties().setPromptColor("black");
                textIO.dispose();
            }
        }
    }
}
