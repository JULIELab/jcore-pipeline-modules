package de.julielab.jcore.pipeline.runner.application;

import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.jcore.pipeline.builder.base.exceptions.PipelineIOException;
import de.julielab.jcore.pipeline.runner.services.PipelineRunnerService;
import de.julielab.jcore.pipeline.runner.util.PipelineInstantiationException;
import de.julielab.jcore.pipeline.runner.util.PipelineRunningException;
import de.julielab.jssf.commons.spi.ConfigurationTemplateGenerator;
import de.julielab.jssf.commons.util.ConfigurationException;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.configuration2.tree.xpath.XPathExpressionEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static de.julielab.jcore.pipeline.runner.util.PipelineRunnerConstants.*;

public class PipelineRunnerCLI implements ConfigurationTemplateGenerator {
    private final static Logger log = LoggerFactory.getLogger(PipelineRunnerCLI.class);

    public static void main(String args[]) {
        if (args.length != 1) {
            log.error("Usage: {} <XML configuration file path; configuration template will be written if " +
                    "path does not exist>", PipelineRunnerCLI.class.getSimpleName());
            System.exit(1);
        }
        File configurationFile = new File(args[0]);
        PipelineRunnerCLI pipelineRunnerCLI = new PipelineRunnerCLI();
        pipelineRunnerCLI.run(configurationFile);
    }

    private void run(File configurationFile) {
        if (configurationFile.exists()) {
            PipelineRunnerService runnerService = PipelineRunnerService.getInstance();
            try {
                XMLConfiguration configuration = ConfigurationUtilities.loadXmlConfiguration(configurationFile);
                configuration.setExpressionEngine(new XPathExpressionEngine());
                for (HierarchicalConfiguration<ImmutableNode> runnerConfig :
                        configuration.configurationsAt(slash(RUNNERS, RUNNER)))
                    runnerService.runPipeline(runnerConfig);
            } catch (PipelineInstantiationException e) {
                log.error("The given pipeline could not be created: {}", e.getMessage());
            } catch (PipelineRunningException e) {
                log.error("Pipeline crashed:", e);
                log.error("The given pipeline could not be run: {}", e.getMessage());
            } catch (org.apache.commons.configuration2.ex.ConfigurationException e) {
                log.error("Configuration file {} could not be read: {}", configurationFile, e.getMessage());
            } catch (PipelineIOException e) {
                e.printStackTrace();
                log.error("One of the pipelines given in the configuration file {} could not be loaded: ",
                        configurationFile, e.getMessage());
            }
        } else {
            log.warn("The configuration file {} does not exist. A configuration template is written " +
                    "to this location that can be used as a starting point for a custom " +
                    "configuration.", configurationFile);
            try {
                writeConfigurationTemplate(configurationFile);
            } catch (ConfigurationException e) {
                log.error("Configuration template could not be written: " + e.getMessage());
                log.debug("Complete exception:", e);
            }
        }
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
       PipelineRunnerService.getInstance().exposeParameters(slash(RUNNERS, RUNNER), template);
    }
}
