package de.julielab.jcore.pipeline.runner;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static de.julielab.jcore.pipeline.runner.util.PipelineRunnerConstants.NAME;
import static de.julielab.jcore.pipeline.runner.util.PipelineRunnerConstants.NUMTHREADS;
import static de.julielab.jcore.pipeline.runner.util.PipelineRunnerConstants.PIPELINEPATH;
import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;


public class CPEBootstrapRunner implements IPipelineRunner {

    private final static Logger log = LoggerFactory.getLogger(CPEBootstrapRunner.class);

    @Override
    public void runPipeline(JCoReUIMAPipeline pipeline, HierarchicalConfiguration<ImmutableNode> runnerConfig) throws PipelineRunningException, PipelineIOException {
        try {
            pipeline.load(false);
            final String plp = pipeline.getLoadDirectory().getAbsolutePath();
            int numThreads = runnerConfig.containsKey(NUMTHREADS) ? runnerConfig.getInt(NUMTHREADS) : 2;
            final File cpeRunnerJar = findCpeRunnerJar();
            Stream<File> classpathElements = pipeline.getClasspathElements();
            classpathElements = Stream.concat(classpathElements, Stream.of(cpeRunnerJar, new File(plp + File.separator + JCoReUIMAPipeline.DIR_CONF), new File(plp + File.separator + "resources")));
            String classpath = classpathElements.map(File::getAbsolutePath).collect(Collectors.joining(File.pathSeparator));

            final String[] cmdarray = {"java", "-cp", classpath, "de.julielab.jcore.pipeline.runner.cpe.CPERunner", "-d", plp + File.separator +  JCoReUIMAPipeline.DIR_DESC + File.separator + "CPE.xml", "-t", String.valueOf(numThreads)};
            log.info("Running the pipeline at {} with the following command line: {}", pipeline.getLoadDirectory(), Arrays.toString(cmdarray));
            final Process exec = Runtime.getRuntime().exec(cmdarray);
            final InputStreamGobbler isg = new InputStreamGobbler(exec.getInputStream(), "StdInGobbler", "std");
            isg.start();
            final InputStreamGobbler errg = new InputStreamGobbler(exec.getErrorStream(), "ErrInGobbler", "err");
            errg.start();

            final int i = exec.waitFor();
            if (i != 0) {
                isg.join();
                errg.join();
                throw new RuntimeException("Pipeline runner process exited with status " + i);
            }

        } catch (IOException e) {
            throw new PipelineRunningException(e);
        } catch (InterruptedException e) {
            throw new PipelineRunningException(e);
        } finally {
            log.info("Pipeline run completed.");
        }
    }

    private class InputStreamGobbler extends Thread {
        private InputStream is;
        private String type;

        public InputStreamGobbler(InputStream is, String threadName, String type) {
            this.is = is;
            this.type = type;
            setName(threadName);
        }

        @Override
        public void run() {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (type.equals("std")) {
                        System.out.println(line);
                    } else {
                        System.err.println(line);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private File findCpeRunnerJar() {
        String classpath = System.getProperty("java.class.path");
        Stream<File> classpathDirs = Stream.of(classpath.split(File.pathSeparator)).map(File::new).map(f -> f.isDirectory() ? f : f.getParentFile()).distinct();

        final Stream<File> dirsToCheck = Stream.concat(Stream.of(new File(".")), classpathDirs);
        final Optional<File> pipelineRunnerJar = dirsToCheck.flatMap(dir -> Stream.of(dir.listFiles(((dir1, name) -> name.startsWith("jcore-pipeline-runner-cpe"))))).findFirst();
        if (pipelineRunnerJar.isPresent()) {
            final File cpeRunner = pipelineRunnerJar.get();
            log.info("Found JCoRe CPE runner at {}", cpeRunner);
            return cpeRunner;
        } else {
            throw new IllegalStateException("The CPE runner JAR could not be located. It must be colocated with any file on the classpath, i.e.");
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
        CPEBootstrapRunner.StatusCallbackListenerImpl status = new CPEBootstrapRunner.StatusCallbackListenerImpl();

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
