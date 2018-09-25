package de.julielab.jcore.pipeline.runner;

import de.julielab.java.utilities.classpath.JarLoader;
import de.julielab.jcore.pipeline.builder.base.exceptions.PipelineIOException;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.runner.spi.IPipelineRunner;
import de.julielab.jcore.pipeline.runner.util.PipelineInstantiationException;
import de.julielab.jcore.pipeline.runner.util.PipelineRunningException;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionProcessingEngine;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.collection.EntityProcessStatus;
import org.apache.uima.collection.StatusCallbackListener;
import org.apache.uima.collection.metadata.CpeDescription;
import org.apache.uima.collection.metadata.CpeDescriptorException;
import org.apache.uima.fit.cpe.CpeBuilder;
import org.apache.uima.util.ProcessTrace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static de.julielab.jcore.pipeline.runner.util.PipelineRunnerConstants.NAME;
import static de.julielab.jcore.pipeline.runner.util.PipelineRunnerConstants.NUMTHREADS;
import static de.julielab.jcore.pipeline.runner.util.PipelineRunnerConstants.PIPELINEPATH;
import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;


public class CPERunner implements IPipelineRunner {

    private final static Logger log = LoggerFactory.getLogger(CPERunner.class);

    @Override
    public void runPipeline(JCoReUIMAPipeline pipeline, HierarchicalConfiguration<ImmutableNode> runnerConfig) throws PipelineInstantiationException, PipelineRunningException, PipelineIOException {
        try {
            Stream<File> classpathElements = pipeline.getClasspathElements();
            classpathElements.forEach(JarLoader::addJarToClassPath);
            pipeline.load(false);
            int numThreads = runnerConfig.containsKey(NUMTHREADS) ? runnerConfig.getInt(NUMTHREADS) : 2;
            // The CpePipeline.runPipeline() code was checked for the number of threads.
            log.info("Running pipeline with {} threads.", numThreads);
            if (pipeline.getCcDelegates() != null) {
                if (!(pipeline.getCcDesc() instanceof AnalysisEngineDescription))
                    throw new PipelineInstantiationException("Could not create CPE because the CasConsumer descriptor does not " +
                            "implement the AnalysisEngineDescription interface. The CasConsumerDescription interface is " +
                            "deprecated and not used by UIMAfit which is employed by this class to build the CPE.");
                runPipeline(numThreads,
                        pipeline.getCrDescription().getDescriptorAsCollectionReaderDescription(),
                        pipeline.getCompleteAggregateDescription());
            } else {
                runPipeline(numThreads, pipeline.getCrDescription().getDescriptorAsCollectionReaderDescription(), pipeline.getCompleteAggregateDescription());
            }
        } catch (UIMAException | SAXException e) {
            throw new PipelineRunningException(e);
        } catch (CpeDescriptorException e) {
            throw new PipelineInstantiationException(e);
        } catch (IOException e) {
            throw new PipelineRunningException(e);
        } finally {
            log.info("Pipeline run completed.");
        }
    }


    @Override
    public String getName() {
        return "CPERunner";
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        template.addProperty(slash(basePath, NAME), getName());
        template.addProperty(slash(basePath, PIPELINEPATH), "");
        template.addProperty(slash(basePath, NUMTHREADS), "2");
    }


    private void runPipeline(int numThreads, final CollectionReaderDescription readerDesc,
                             final AnalysisEngineDescription... descs) throws UIMAException, SAXException,
            CpeDescriptorException, IOException {
        // Create AAE
        final AnalysisEngineDescription aaeDesc = createEngineDescription(descs);

        CpeBuilder builder = new CpeBuilder();
        builder.setReader(readerDesc);
        builder.setAnalysisEngine(aaeDesc);
        builder.setMaxProcessingUnitThreadCount(numThreads);
        CPERunner.StatusCallbackListenerImpl status = new CPERunner.StatusCallbackListenerImpl();

        CpeDescription cpeDescription = builder.getCpeDescription();
        Stream.of(cpeDescription.getCpeCasProcessors().getAllCpeCasProcessors()).forEach(cp -> cp.setBatchSize(100));
        CollectionProcessingEngine engine = builder.createCpe(status);

        engine.process();
        try {
            synchronized (status) {
                while (status.isProcessing) {
                    status.wait();
                }
            }
        } catch (InterruptedException e) {
            // Do nothing
        }

        log.info("Pipeline Performance report: {}, {}", System.getProperty("line.separator"), engine.getPerformanceReport());
        if (status.exceptions.size() > 0) {
            throw new AnalysisEngineProcessException(status.exceptions.get(0));
        }
    }

    private static class StatusCallbackListenerImpl implements StatusCallbackListener {

        private final List<Exception> exceptions = new ArrayList<Exception>();

        private boolean isProcessing = true;

        public void entityProcessComplete(CAS arg0, EntityProcessStatus arg1) {
            if (arg1.isException()) {
                for (Exception e : arg1.getExceptions()) {
                    exceptions.add(e);
                }
            }
        }

        public void aborted() {
            synchronized (this) {
                if (isProcessing) {
                    isProcessing = false;
                    notify();
                }
            }
        }

        public void batchProcessComplete() {
            // Do nothing
        }

        public void collectionProcessComplete() {
            log.info("Processing of all documents is done, calling collectionProcessComplete().");
            synchronized (this) {
                if (isProcessing) {
                    isProcessing = false;
                    notify();
                }
            }
        }

        public void initializationComplete() {
            // Do nothing
        }

        public void paused() {
            // Do nothing
        }

        public void resumed() {
            // Do nothing
        }
    }
}