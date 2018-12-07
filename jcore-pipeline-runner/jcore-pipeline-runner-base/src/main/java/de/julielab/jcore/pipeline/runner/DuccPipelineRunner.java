package de.julielab.jcore.pipeline.runner;

import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.java.utilities.FileUtilities;
import de.julielab.jcore.pipeline.builder.base.exceptions.PipelineIOException;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.runner.spi.IPipelineRunner;
import de.julielab.jcore.pipeline.runner.util.PipelineRunningException;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.ducc.cli.DuccJobSubmit;
import org.apache.uima.ducc.transport.event.cli.JobSpecificationProperties;
import org.apache.uima.resource.ResourceSpecifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.stream.Stream;

import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static de.julielab.jcore.pipeline.runner.util.PipelineRunnerConstants.*;

/**
 * Creates a DUCC job from a given configuration. The configuration must contain the path to the pipeline.
 * It may also contain the path to a DUCC job file and DUCC job file overrides.
 * These items are resolved in the following order:
 * <ol>
 * <li>Loading the DUCC job file, if given</li>
 * <li>Setting job file properties given by the configuration</li>
 * <li>Setting job file properties derived from the pipeline itself:
 * <ul>
 * <li>Descriptor locations for Collection Reader, (Aggregate) Analysis Engine and CAS Consumer</li>
 * <li>Classpath to the required libraries</li>
 * </ul>
 * </li>
 * </ol>
 * Configurations added at a later stage override those added at an earlier stage. Thus, pipeline settings always take
 * precedence over all other configuration values.
 */
public class DuccPipelineRunner implements IPipelineRunner {

    private final static Logger log = LoggerFactory.getLogger(DuccPipelineRunner.class);

    @Override
    public void runPipeline(JCoReUIMAPipeline pipeline, HierarchicalConfiguration<ImmutableNode> runnerConfig) throws PipelineRunningException {
        try {
            checkPipeline(pipeline);
            Properties props = loadJobFile(runnerConfig);
            setJobPropertiesFromConfiguration(runnerConfig, props);
            setPropertiesFromPipeline(pipeline, props);

            DuccJobSubmit jobSubmit = new DuccJobSubmit(props);
            if (jobSubmit.execute())
                log.info("Successfully submitted job number {} to DUCC. ", jobSubmit.getDuccId());
            else
                log.error("Failed to submit the job to DUCC. Return code: {}", jobSubmit.getReturnCode());

        } catch (Exception e) {
            throw new PipelineRunningException(e);
        }
    }

    private void checkPipeline(JCoReUIMAPipeline pipeline) {
        AnalysisEngineDescription aaeDesc = pipeline.getAaeDesc();
        checkDescriptor(aaeDesc);
    }

    private void checkDescriptor(ResourceSpecifier descriptor) {
        // TODO check for multipleDeploymentAllowed=false and throw error
    }


    private void setPropertiesFromPipeline(JCoReUIMAPipeline pipeline, Properties props) throws PipelineIOException {
        log.debug("Setting properties derived from the pipeline." );
        if (pipeline.getCrDescription() != null && pipeline.getCrDescription().getDescriptor().getSourceUrl() != null) {
            log.debug("Setting {}: {}", JobSpecificationProperties.key_driver_descriptor_CR, pipeline.getCrDescription().getDescriptor().getSourceUrl().toString());
            props.setProperty(JobSpecificationProperties.key_driver_descriptor_CR, pipeline.getCrDescription().getDescriptor().getSourceUrl().toString());
        }
        if (pipeline.getAaeCmDesc() != null && pipeline.getAaeCmDesc().getSourceUrl() != null) {
            log.debug("Setting {}: {}", JobSpecificationProperties.key_process_descriptor_CM, pipeline.getAaeCmDesc().getSourceUrl().toString());
            props.setProperty(JobSpecificationProperties.key_process_descriptor_CM, pipeline.getAaeCmDesc().getSourceUrl().toString());
        }
        if (pipeline.getAaeDesc() != null && pipeline.getAaeDesc().getSourceUrl() != null) {
            log.debug("Setting {}: {}", JobSpecificationProperties.key_process_descriptor_AE, pipeline.getAaeDesc().getSourceUrl().toString());
            props.setProperty(JobSpecificationProperties.key_process_descriptor_AE, pipeline.getAaeDesc().getSourceUrl().toString());
        }
        if (pipeline.getCcDesc() != null && pipeline.getCcDesc().getSourceUrl() != null) {
            log.debug("Setting {}: {}", JobSpecificationProperties.key_process_descriptor_CC, pipeline.getCcDesc().getSourceUrl().toString());
            props.setProperty(JobSpecificationProperties.key_process_descriptor_CC, pipeline.getCcDesc().getSourceUrl().toString());
        }
        props.setProperty(JobSpecificationProperties.key_classpath, pipeline.getClassPath());
    }

    private void setJobPropertiesFromConfiguration(HierarchicalConfiguration<ImmutableNode> runnerConfig, Properties props) throws ConfigurationException {
        HierarchicalConfiguration<ImmutableNode> jobDefinitionConfig = runnerConfig.configurationAt(slash(CONFIGURATION, JOBDESCRIPTION));
        Iterator<String> keys = jobDefinitionConfig.getKeys();
        while (keys.hasNext()) {
            String key = keys.next();
            String value = jobDefinitionConfig.getString(key);
            if (value != null && !value.isEmpty())
                props.setProperty(key, value);
        }
        String duccHome = ConfigurationUtilities.requirePresent(DUCCHOMEPATH, runnerConfig::getString);
        System.setProperty("DUCC_HOME", duccHome);
    }

    /**
     * Loads the job file, if specified, and returns the respective properties. Returns an empty properties instance if
     * no job file is given.
     *
     * @param runnerConfig The runner configuration, i.e. the configuration with root &lt;runner&gt;.
     * @return The DUCC job description or an empty properties if no job file is given.
     * @throws IOException If reading the job file fails.
     */
    private Properties loadJobFile(HierarchicalConfiguration<ImmutableNode> runnerConfig) throws IOException, ConfigurationException {
        String jobFilePath = runnerConfig.getString(slash(CONFIGURATION, JOBFILE));
        Properties props = new Properties();
        if (jobFilePath != null) {
            File jobFile = new File(jobFilePath);
            ConfigurationUtilities.checkFilesExist(runnerConfig, jobFilePath);
            props.load(FileUtilities.getInputStreamFromFile(jobFile));
        }
        return props;
    }

    @Override
    public String getName() {
        return "DuccRunner";
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        template.addProperty(slash(basePath, NAME), getName());
        template.addProperty(slash(basePath, PIPELINEPATH), "path to JCoRePipeline (optional if job description is complete)");
        template.addProperty(slash(basePath, DUCCHOMEPATH), "path to the home directory of the DUCC installation of the shared file system  ");
        template.addProperty(slash(basePath, CONFIGURATION, JOBFILE), "optional");
        template.addProperty(slash(basePath, CONFIGURATION, JOBDESCRIPTION), "");
        Stream<String> keys = Stream.of(JobSpecificationProperties.keys);
        // We also add a few more keys that, for some reason, have not been included into the
        // JobSpecificationProperties.keys array. This is still not exhaustive. But perhaps it doesn't have to be.
        keys = Stream.concat(keys, Stream.of(
                JobSpecificationProperties.key_classpath,
                JobSpecificationProperties.key_environment,
                JobSpecificationProperties.key_process_pipeline_count));
        keys.forEach(key -> template.addProperty(slash(basePath, CONFIGURATION, JOBDESCRIPTION, key), ""));
        // And a few helpful hints
        template.setProperty(slash(basePath, CONFIGURATION, JOBDESCRIPTION, JobSpecificationProperties.key_process_memory_size), "maximum amount of memory in GB");
    }
}
