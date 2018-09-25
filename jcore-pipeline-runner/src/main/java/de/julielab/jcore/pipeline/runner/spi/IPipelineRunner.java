package de.julielab.jcore.pipeline.runner.spi;

import de.julielab.jcore.pipeline.builder.base.exceptions.PipelineIOException;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.runner.util.PipelineInstantiationException;
import de.julielab.jcore.pipeline.runner.util.PipelineRunningException;
import de.julielab.jssf.commons.spi.ExtensionPoint;
import de.julielab.jssf.commons.spi.ParameterExposing;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

public interface IPipelineRunner extends ParameterExposing, ExtensionPoint {
    void runPipeline(JCoReUIMAPipeline pipeline, HierarchicalConfiguration<ImmutableNode> runnerConfig) throws PipelineInstantiationException, PipelineRunningException, PipelineIOException;

}
