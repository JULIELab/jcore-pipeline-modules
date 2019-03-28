package de.julielab.jcore.pipeline.runner.services;

import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.jcore.pipeline.builder.base.exceptions.PipelineIOException;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.runner.spi.IPipelineRunner;
import de.julielab.jcore.pipeline.runner.util.PipelineInstantiationException;
import de.julielab.jcore.pipeline.runner.util.PipelineRunningException;
import de.julielab.jssf.commons.spi.ParameterExposing;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.util.Iterator;
import java.util.ServiceLoader;

import static de.julielab.java.utilities.ConfigurationUtilities.last;
import static de.julielab.jcore.pipeline.runner.util.PipelineRunnerConstants.NAME;
import static de.julielab.jcore.pipeline.runner.util.PipelineRunnerConstants.PIPELINEPATH;

public class PipelineRunnerService implements ParameterExposing{
    private static PipelineRunnerService service;
    private ServiceLoader<IPipelineRunner> loader;

    public static PipelineRunnerService getInstance() {
        if (service == null)
            service = new PipelineRunnerService();
        return service;
    }

    private PipelineRunnerService() {
        loader = ServiceLoader.load(IPipelineRunner.class);
    }

    public void runPipeline(JCoReUIMAPipeline pipeline, HierarchicalConfiguration<ImmutableNode> runnerConfig) throws PipelineInstantiationException, PipelineRunningException, ConfigurationException, PipelineIOException {
        String runnerName = ConfigurationUtilities.requirePresent(NAME, runnerConfig::getString);
        Iterator<IPipelineRunner> runnerIt = loader.iterator();
        boolean runnerFound = false;
        while (runnerIt.hasNext()) {
            IPipelineRunner runner = runnerIt.next();
            if (runner.hasName(runnerName)) {
                runnerFound = true;
                runner.runPipeline(pipeline, runnerConfig);
            }
        }
        if (!runnerFound)
            throw new PipelineRunningException("No runner with name " + runnerName + " was found. Make sure to add the " +
                    "qualified Java name of the desired runner to the service configuration at " +
                    "META-INF/services/de.julielab.jcore.pipeline.runner.spi.IPipelineRunner");
    }

    public void runPipeline(HierarchicalConfiguration<ImmutableNode> runnerConfig) throws ConfigurationException, PipelineIOException, PipelineRunningException, PipelineInstantiationException {
        String pipelinePath = ConfigurationUtilities.requirePresent(PIPELINEPATH, runnerConfig::getString);
        JCoReUIMAPipeline jCoReUIMAPipeline = new JCoReUIMAPipeline(new File(pipelinePath));
        //Stream<File> classpathElements = jCoReUIMAPipeline.getClasspathElements();
        //classpathElements.forEach(JarLoader::addJarToClassPath);
        jCoReUIMAPipeline.load(false);
        runPipeline(jCoReUIMAPipeline,runnerConfig );
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        Iterator<IPipelineRunner> runnerIt = loader.iterator();
        while (runnerIt.hasNext()) {
            template.addProperty(basePath, "");
            IPipelineRunner runner = runnerIt.next();
            runner.exposeParameters(last(basePath), template);
        }
    }
}
