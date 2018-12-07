package de.julielab.jcore.pipeline.runner.util;

import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.jcore.pipeline.builder.base.exceptions.PipelineIOException;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.runner.services.PipelineRunnerService;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;

import static de.julielab.java.utilities.ConfigurationUtilities.*;

public class PipelineRunnerUtilities {

    public static JCoReUIMAPipeline createPipeline(HierarchicalConfiguration<ImmutableNode> runnerConfig) throws ConfigurationException, PipelineIOException {
        String pipelinePath = requirePresent(PipelineRunnerConstants.NAME, runnerConfig::getString);
        return new JCoReUIMAPipeline(new File(pipelinePath));
    }
}
