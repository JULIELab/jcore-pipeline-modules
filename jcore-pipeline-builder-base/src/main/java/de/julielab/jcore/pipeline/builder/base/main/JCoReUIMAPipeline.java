package de.julielab.jcore.pipeline.builder.base.main;

import com.google.common.collect.Sets;
import de.julielab.java.utilities.FileUtilities;
import de.julielab.java.utilities.classpath.JarLoader;
import de.julielab.jcore.pipeline.builder.base.PipelineParameterChecker;
import de.julielab.jcore.pipeline.builder.base.connectors.MavenConnector;
import de.julielab.jcore.pipeline.builder.base.exceptions.MavenException;
import de.julielab.jcore.pipeline.builder.base.exceptions.PipelineIOException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.metadata.AnalysisEngineMetaData;
import org.apache.uima.analysis_engine.metadata.FixedFlow;
import org.apache.uima.analysis_engine.metadata.FlowConstraints;
import org.apache.uima.analysis_engine.metadata.impl.FixedFlow_impl;
import org.apache.uima.collection.CasConsumer;
import org.apache.uima.collection.CasConsumerDescription;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.collection.impl.metadata.cpe.CpeCasProcessorsImpl;
import org.apache.uima.collection.impl.metadata.cpe.CpeComponentDescriptorImpl;
import org.apache.uima.collection.impl.metadata.cpe.CpeIntegratedCasProcessorImpl;
import org.apache.uima.collection.metadata.CpeComponentDescriptor;
import org.apache.uima.collection.metadata.CpeDescription;
import org.apache.uima.collection.metadata.CpeDescriptorException;
import org.apache.uima.fit.cpe.CpeBuilder;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.resource.ResourceCreationSpecifier;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.Import;
import org.apache.uima.resource.metadata.MetaDataObject;
import org.apache.uima.resource.metadata.ResourceMetaData;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.resource.metadata.impl.Import_impl;
import org.apache.uima.resource.metadata.impl.TypeSystemDescription_impl;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.XMLInputSource;
import org.apache.uima.util.XMLParser;
import org.eclipse.aether.artifact.Artifact;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.*;

public class JCoReUIMAPipeline {
    public static final String DIR_DESC = "desc";
    public static final String DIR_LIB = "lib";
    public static final String DIR_CONF = "config";
    public static final String CPE_AAE_DESC_NAME = "cpeAAE.xml";
    public static final String JAR_CONF_FILES = "jcore-pipeline-config.jar";
    private static final String SERIALIZED_CR_DESCS_FILE = "crDescriptions.bin";
    private static final String SERIALIZED_CM_DESCS_FILE = "cmDescriptions.bin";
    private static final String SERIALIZED_AE_DESCS_FILE = "aeDescriptions.bin";
    private static final String SERIALIZED_CC_DESCS_FILE = "ccDescriptions.bin";
    private final static Logger log = LoggerFactory.getLogger(JCoReUIMAPipeline.class);

    private static Function<List<Description>, Stream<Import>> tsImportsExtractor = descs -> descs.stream().flatMap(desc -> {
        final AnalysisEngineMetaData analysisEngineMetaData = desc.getDescriptorAsAnalysisEngineDescription().getAnalysisEngineMetaData();
        if (analysisEngineMetaData == null)
            return null;
        final TypeSystemDescription typeSystem = analysisEngineMetaData.getTypeSystem();
        if (typeSystem == null)
            return null;
        return Stream.of(typeSystem.getImports());
    }).filter(Objects::nonNull);

    private Description crDescription;
    private List<Description> aeDelegates;
    private List<Description> cmDelegates;
    private List<Description> ccDelegates;

    private CollectionReaderDescription crDesc;
    /**
     * All multipliers, analysis engines and, if possible, consumers are wrapped into an AAE.
     */
    private AnalysisEngineDescription aaeDesc;
    private AnalysisEngineDescription aaeCmDesc;
    private ResourceCreationSpecifier ccDesc;

    /**
     * This file is only non-null when the pipeline has been loaded.
     */
    private File loadDirectory;
    /**
     * We keep track components removed from the pipeline. If the respective changes are stored to disc,
     * the respective descriptor files ought to be removed.
     */
    private Set<String> filesToDeleteOnSave = new HashSet<>();

    /**
     * <p>
     * Set the pipeline base directory to later load the pipeline from. It is expected that this directory has two subdirectories named
     * {@value DIR_DESC} and {@value DIR_LIB}, respectively. The first must contain the configured descriptors
     * of collection reader, analysis engines (including CAS multipliers) and an optional CAS consumer. The latter must
     * contain all the artifacts and transitive dependencies required to run the pipeline.
     * </p>
     * <p>To actually load the pipeline, call {@link #load(boolean)}</p>
     *
     * @param baseDirectory The base directory where to find the subdirectories {@value DIR_DESC} and {@value DIR_LIB}.
     * @see #load(boolean)
     */
    public JCoReUIMAPipeline(File baseDirectory) {
        this();
        setLoadDirectory(baseDirectory);
    }

    /**
     * Empty constructur for creating a pipeline from scratch.
     */
    public JCoReUIMAPipeline() {
        cmDelegates = new ArrayList<>();
        aeDelegates = new ArrayList<>();
        ccDelegates = new ArrayList<>();
    }

    public Stream<Description> getMultipliers() {
        return aeDelegates.stream().
                filter(d -> ((AnalysisEngineDescription) d.getDescriptor()).getAnalysisEngineMetaData().getOperationalProperties().getOutputsNewCASes());
    }

    public Stream<Description> getNonMultiplierAes() {
        return aeDelegates.stream().
                filter(d -> !((AnalysisEngineDescription) d.getDescriptor()).getAnalysisEngineMetaData().getOperationalProperties().getOutputsNewCASes());
    }

    public Description getCrDescription() {
        return crDescription;
    }

    public void setCrDescription(Description crDescription) {
        this.crDescription = crDescription;
        this.crDesc = crDescription.getDescriptorAsCollectionReaderDescription();
    }

    /**
     * The single primitive analysis engine description or the aggregate analysis engine description of this pipeline.
     *
     * @return The analysis engine description.
     */
    public AnalysisEngineDescription getAaeDesc() {
        return aaeDesc;
    }

    public List<Description> getCcDelegates() {
        return ccDelegates;
    }

    public void addCcDesc(Description ccDesc) {
        if (ccDelegates.stream().map(Description::getDescriptor).filter(CasConsumerDescription.class::isInstance).findAny().isPresent())
            throw new IllegalArgumentException("There is already a consumer represented by a " +
                    " " + CasConsumerDescription.class.getCanonicalName() + ". " +
                    "Those are deprecated and only one can be used in each pipeline.");
        this.ccDelegates.add(ccDesc);
    }

    public void store(File directory) throws PipelineIOException {
        store(directory, false);
    }

    public void store(File directory, boolean clearLibDir) throws PipelineIOException {

        String message = "";
        if ((aaeDesc == null && (aeDelegates == null || aeDelegates.isEmpty()) && ccDesc == null && (ccDelegates == null || ccDelegates.isEmpty())) || crDescription == null) {
            message = "This pipeline has either no collection reader or no analysis engines and no consumer. " +
                    "A reader and an analysis engine or a consumer is required to do any work. The pipeline will be stored anyway but will need additional work.";
            log.warn(message);
        }

        // Load the libraries for this pipeline. They are required for aggregate engine creation.
        getClasspathElements().forEach(JarLoader::addJarToClassPath);

        // Store descriptors
        try {
            File descDir = new File(directory.getAbsolutePath() + File.separator + DIR_DESC);
            if (!descDir.exists())
                descDir.mkdirs();
            if (!aeDelegates.isEmpty()) {
                Stream<AnalysisEngineDescription> descStream = aeDelegates.stream().
                        filter(desc -> !desc.getMetaDescription().isPear()).
                        map(Description::getDescriptorAsAnalysisEngineDescription);
                aaeDesc = AnalysisEngineFactory.createEngineDescription(descStream.toArray(AnalysisEngineDescription[]::new));
                Map<String, MetaDataObject> delegatesWithImports = aaeDesc.getDelegateAnalysisEngineSpecifiersWithImports();
                delegatesWithImports.clear();
                List<String> flowNames = new ArrayList<>();
                for (int i = 0; i < aeDelegates.size(); ++i) {
                    Description description = aeDelegates.get(i);
                    if (!description.getMetaDescription().isPear()) {
                        Import imp = new Import_impl();
                        imp.setLocation(description.getName() + ".xml");
                        delegatesWithImports.put(description.getName(), imp);
                        final File destination = new File(descDir.getAbsolutePath() + File.separator + description.getName() + ".xml");
                        description.getDescriptorAsAnalysisEngineDescription().toXML(FileUtilities.getWriterToFile(destination), true);
                        flowNames.add(description.getName());
                    } else {
                        Import_impl imp = new Import_impl();
                        File pearDescriptorFile = new File(description.getLocation());
                        imp.setLocation(pearDescriptorFile.toURI().toString());
                        delegatesWithImports.put(description.getName(), imp);
                        flowNames.add(description.getName());
                    }
                }
                // necessary for relative resolution of imports
                aaeDesc.setSourceUrl(descDir.toURI().toURL());
                // This is required when using PEARs. This call causes the internal call to resolveDelegateAnalysisEngineImports()
                // which completes PEAR imports.
                aaeDesc.getDelegateAnalysisEngineSpecifiers();
                ((FixedFlow) aaeDesc.getAnalysisEngineMetaData().getFlowConstraints()).setFixedFlow(flowNames.toArray(new String[flowNames.size()]));
            }
            if (cmDelegates != null && cmDelegates.size() > 1) {
                Stream<AnalysisEngineDescription> descStream = cmDelegates.stream().
                        map(Description::getDescriptorAsAnalysisEngineDescription);
                aaeCmDesc = AnalysisEngineFactory.createEngineDescription(descStream.toArray(AnalysisEngineDescription[]::new));
            }
            File crFile = null;
            if (crDescription != null) {
                final String descriptorFilename = crDescription.getName() + ".xml";
                String pathname = descDir.getAbsolutePath() +
                        File.separator + descriptorFilename;
                crFile = new File(pathname);
                crDescription.getDescriptor().setSourceUrl(crFile.toURI().toURL());
                crDescription.getDescriptor().toXML(FileUtilities.getWriterToFile(
                        crFile));
                crDescription.setUimaDescPath(descriptorFilename);
                filesToDeleteOnSave.remove(descriptorFilename);
            }
            File cmFile = new File(descDir.getAbsolutePath() +
                    File.separator + "AggregateMultiplier.xml");
            if (cmDelegates != null && cmDelegates.size() == 1) {
                Description cm = cmDelegates.get(0);
                aaeCmDesc = cm.getDescriptorAsAnalysisEngineDescription();
                final String descriptorFilename = cm.getName() + ".xml";
                String pathname = descDir.getAbsolutePath() +
                        File.separator + descriptorFilename;
                cmFile = new File(pathname);
                cm.getDescriptor().setSourceUrl(cmFile.toURI().toURL());
                cm.getDescriptorAsAnalysisEngineDescription().toXML(FileUtilities.getWriterToFile(
                        cmFile), true);
                cm.setUimaDescPath(descriptorFilename);
                filesToDeleteOnSave.remove(descriptorFilename);
            } else if (cmDelegates != null && cmDelegates.size() > 1) {
                aaeCmDesc.setSourceUrl(cmFile.toURI().toURL());
                aaeCmDesc.getMetaData().setName("JCoRe Multiplier AAE");
                aaeCmDesc.getMetaData().setDescription("This AAE descriptor directly contains the CAS multipliers added " +
                        "through the JCoRe pipeline builder. The AAE serves to bundle all the components together.");
                aaeCmDesc.toXML(FileUtilities.getWriterToFile(
                        cmFile), true);
            } else if (cmFile.exists()) {
                cmFile.delete();
            }

            File aaeFile = new File(descDir.getAbsolutePath() +
                    File.separator + "AggregateAnalysisEngine.xml");
            if (aaeDesc != null) {
                aaeDesc.setSourceUrl(aaeFile.toURI().toURL());
                aaeDesc.getMetaData().setName("JCoRe Pipeline AAE");
                aaeDesc.getMetaData().setDescription("This AAE descriptor directly contains the analysis engines added " +
                        "through the JCoRe pipeline builder. The AAE serves to bundle all the components together.");
                aaeDesc.toXML(FileUtilities.getWriterToFile(
                        aaeFile), true);
            } else if (aaeFile.exists()) {
                aaeFile.delete();
            }

            File ccFile = new File(descDir.getAbsolutePath() +
                    File.separator +
                    "AggregateConsumer.xml");
            if (ccFile.exists())
                ccFile.delete();
            if (ccDelegates != null && !ccDelegates.isEmpty()) {
                for (Description ccDesc : ccDelegates)
                    storeCCDescriptor(ccDesc, descDir);
                if (ccDelegates.size() == 1) {
                    ccDesc = (ResourceCreationSpecifier) ccDelegates.get(0).getDescriptor();
                    ccFile = new File(descDir.getAbsolutePath() + File.separator + ccDelegates.get(0).getUimaDescPath());
                } else if (ccDelegates.size() > 1) {
                    // Create an empty aggregate
                    ccDesc = AnalysisEngineFactory.createEngineDescription();
                    AnalysisEngineDescription ccAAE = (AnalysisEngineDescription) ccDesc;
                    Map<String, MetaDataObject> delegateAnalysisEngineSpecifiersWithImports = ccAAE.getDelegateAnalysisEngineSpecifiersWithImports();
                    // Add the delegates to the aggregate via imports
                    for (Description desc : ccDelegates) {
                        AnalysisEngineDescription ae = desc.getDescriptorAsAnalysisEngineDescription();
                        Import_impl aeImport = new Import_impl();
                        aeImport.setLocation(new File(desc.getUri()).getName());
                        aeImport.setSourceUrl(desc.getUri().toURL());
                        delegateAnalysisEngineSpecifiersWithImports.put(ae.getMetaData().getName(), aeImport);
                    }
                    // Create the AAE flow
                    FixedFlow_impl flow = new FixedFlow_impl();
                    String[] delegateNames = ccDelegates.stream().map(Description::getDescriptorAsAnalysisEngineDescription).map(AnalysisEngineDescription::getMetaData).map(ResourceMetaData::getName).toArray(String[]::new);
                    flow.setFixedFlow(delegateNames);
                    ccAAE.getAnalysisEngineMetaData().setFlowConstraints(flow);
                    ccDesc.setSourceUrl(ccFile.toURI().toURL());
                    ccDesc.getMetaData().setName("JCoRe Consumer AAE");
                    ccDesc.getMetaData().setDescription("This consumer AAE descriptor directly contains the CAS consumers added " +
                            "through the JCoRe pipeline builder. The AAE serves to bundle all the components together.");
                    ccDesc.toXML(FileUtilities.getWriterToFile(
                            ccFile));
                }
            }

            // Storing a CPE descriptor
            try {
                final File cpeAAEFile = new File(descDir.getAbsolutePath() + File.separator + CPE_AAE_DESC_NAME);
                final File cpeFile = new File(descDir.getAbsolutePath() + File.separator +
                        "CPE.xml");
                if (ccDelegates == null || ccDelegates.stream().map(Description::getDescriptor).filter(CasConsumer.class::isInstance).count() == 0) {
                    CpeBuilder builder = new CpeBuilder();
                    if (crDescription != null)
                        builder.setReader(crDescription.getDescriptorAsCollectionReaderDescription());
                    AnalysisEngineDescription cpeAAE = AnalysisEngineFactory.createEngineDescription();

                    cpeAAE.getDelegateAnalysisEngineSpecifiersWithImports().clear();
                    if (!cmDelegates.isEmpty()) {
                        Import_impl cmImport = new Import_impl();
                        cmImport.setLocation(cmFile.getName());
                        cmImport.setSourceUrl(cmFile.toURI().toURL());
                        cpeAAE.getDelegateAnalysisEngineSpecifiersWithImports().put(aaeCmDesc.getMetaData().getName(), cmImport);
                    }
                    if (aaeDesc != null) {
                        Import_impl aaeImport = new Import_impl();
                        aaeImport.setLocation(aaeFile.getName());
                        aaeImport.setSourceUrl(aaeFile.toURI().toURL());
                        cpeAAE.getDelegateAnalysisEngineSpecifiersWithImports().put(aaeDesc.getMetaData().getName(), aaeImport);

                    }
                    if (ccDelegates != null && !ccDelegates.isEmpty()) {
                        Import_impl ccImport = new Import_impl();
                        ccImport.setLocation(ccFile.getName());
                        ccImport.setSourceUrl(ccFile.toURI().toURL());
                        cpeAAE.getDelegateAnalysisEngineSpecifiersWithImports().put(ccDesc.getMetaData().getName(), ccImport);
                    }
                    String[] flow = Stream.of(aaeCmDesc, aaeDesc, ccDesc).filter(Objects::nonNull).map(ResourceCreationSpecifier::getMetaData).map(ResourceMetaData::getName).toArray(String[]::new);
                    ((FixedFlow) cpeAAE.getAnalysisEngineMetaData().getFlowConstraints()).setFixedFlow(flow);
                    cpeAAE.toXML(
                            FileUtilities.getWriterToFile(
                                    cpeAAEFile));
                    builder.setAnalysisEngine(cpeAAE);
                    CpeDescription cpeDescription = builder.getCpeDescription();
                    if (crDescription != null) {
                        Import_impl crImport = new Import_impl();
                        crImport.setLocation(crFile.getName());
                        final CpeComponentDescriptor crDescriptor = cpeDescription.getAllCollectionCollectionReaders()[0].getCollectionIterator().
                                getDescriptor();
                        // delete the automatically generated include; we don't want an include (Absolute URLs are used)
                        // but an import by location (a path relative to the CPE.xml descriptor is used)
                        crDescriptor.setInclude(null);
                        crDescriptor.setImport(crImport);
                    }

                    Import_impl cpeAaeImport = new Import_impl();
                    cpeAaeImport.setLocation(cpeAAEFile.getName());
                    CpeComponentDescriptorImpl cpeComponentDescriptor = new CpeComponentDescriptorImpl();
                    cpeComponentDescriptor.setImport(cpeAaeImport);
                    CpeIntegratedCasProcessorImpl cpeIntegratedCasProcessor = new CpeIntegratedCasProcessorImpl();
                    cpeIntegratedCasProcessor.setCpeComponentDescriptor(cpeComponentDescriptor);
                    cpeIntegratedCasProcessor.setName("CPE AAE");
                    cpeIntegratedCasProcessor.setBatchSize(500);
                    CpeCasProcessorsImpl cpeCasProcessors = new CpeCasProcessorsImpl();
                    cpeCasProcessors.addCpeCasProcessor(cpeIntegratedCasProcessor);
                    cpeDescription.setCpeCasProcessors(cpeCasProcessors);
                    cpeDescription.getCpeCasProcessors().setPoolSize(24);
                    cpeDescription.toXML(FileUtilities.getWriterToFile(
                            cpeFile
                    ));
                } else {
                    log.warn("Could not store a CPE descriptor because a CasConsumer is included in the pipeline that " +
                            "implements a CasConsumer interface rather than the AnalysisEngine interface. Note " +
                            "that CasConsumers are basically analysis engines since UIMA 2.0 and that there is " +
                            "no downside in using AEs as consumers.");
                    if (cpeAAEFile.exists())
                        cpeAAEFile.delete();
                    if (cpeFile.exists())
                        cpeFile.delete();
                }
            } catch (InvalidXMLException e) {
                log.error("Could not store the CPE descriptor: ", e);
            }
        } catch (SAXException | IOException | ResourceInitializationException e) {
            throw new PipelineIOException(e);
        } catch (CpeDescriptorException e) {
            throw new PipelineIOException(e);
        } catch (InvalidXMLException e) {
            e.printStackTrace();
        }

        // Store the list of not-configured mandatory parameters
        List<PipelineParameterChecker.MissingComponentConfiguration> missingConfiguration = PipelineParameterChecker.findMissingConfiguration(this);
        File missingConfigFile = new File(directory.getAbsolutePath() + File.separator + PipelineParameterChecker.MISSING_CONFIG_FILE_NAME);
        if (missingConfigFile.exists())
            missingConfigFile.delete();
        if (!missingConfiguration.isEmpty()) {
            try {
                log.warn("There are missing configuration items for this pipeline. A list of these items is written to {}.", missingConfigFile);
                PipelineParameterChecker.writeMissingConfigurationToFile(missingConfiguration, missingConfigFile);
            } catch (IOException e) {
                log.warn("Could not write the file for missing configuration items", e);
            }
        }

        // Write a file that indicates the pipeline builder version
        InputStream versionFileStream = getClass().getResourceAsStream("/version.txt");
        if (versionFileStream != null) {
            try {
                String version = IOUtils.toString(versionFileStream);
                FileUtils.write(new File(directory.getAbsolutePath() + File.separator + "version-pipelinebuilder.txt"), version + System.getProperty("line.separator"), StandardCharsets.UTF_8.name());
            } catch (IOException e) {
                log.warn("Could not write pipeline builder version to file:", e);
            }
        }

        // Store the required Maven artifacts in the lib directory
        try {
            if (clearLibDir) {
                final File libDir = new File(directory.getAbsolutePath() + File.separator + DIR_LIB);
                log.debug("Removing all files from the library directory at {}", libDir);
                Stream.of(libDir.listFiles()).forEach(File::delete);
            }
            storeArtifacts(directory);
        } catch (MavenException e) {
            throw new PipelineIOException(e);
        }

        // Store descriptions with their meta data
        try {
            serializeDescriptions(directory, SERIALIZED_CR_DESCS_FILE, crDescription);
            serializeDescriptions(directory, SERIALIZED_CM_DESCS_FILE, cmDelegates);
            serializeDescriptions(directory, SERIALIZED_AE_DESCS_FILE, aeDelegates);
            serializeDescriptions(directory, SERIALIZED_CC_DESCS_FILE, ccDelegates);
        } catch (IOException e) {
            throw new PipelineIOException(e);
        }

        // Delete descriptor files of components that have been removed if the storage directory equals the loading directory
        try {
            if (loadDirectory != null && loadDirectory.getCanonicalPath().equals(directory.getCanonicalPath())) {
                for (String toDelete : filesToDeleteOnSave) {
                    final File file = new File(directory.getAbsolutePath() + File.separator + DIR_DESC + File.separator + toDelete);
                    if (file.exists())
                        file.delete();
                }
            }
        } catch (IOException e) {
            throw new PipelineIOException(e);
        }
    }

    private void storeCCDescriptor(Description ccDesc, File descDir) throws SAXException, IOException {
        File ccFile;
        if (!ccDesc.getName().toLowerCase().contains("writer") && !ccDesc.getName().toLowerCase().contains("consumer"))
            throw new IllegalStateException("The CAS consumer descriptor " +
                    ccDesc.getName() + " at " +
                    ccDesc.getDescriptor().getSourceUrlString() + " does not specify 'writer' or 'consumer' " +
                    "in its name. By convention, consumers must do this to be recognized as consumer.");
        final String descriptorFilename = ccDesc.getName() + ".xml";
        ccFile = new File(descDir.getAbsolutePath() +
                File.separator +
                descriptorFilename);
        if (ccDesc.getDescriptor() instanceof AnalysisEngineDescription)
            ccDesc.getDescriptorAsAnalysisEngineDescription().toXML(FileUtilities.getWriterToFile(ccFile), true);
        else
            ccDesc.getDescriptor().toXML(FileUtilities.getWriterToFile(ccFile));
        ccDesc.setUri(ccFile.toURI());
        ccDesc.setUimaDescPath(descriptorFilename);
        filesToDeleteOnSave.remove(descriptorFilename);
    }

    /**
     * Stores the Maven artifacts in the lib/ directory directly beneath the given pipeline <tt>directory</tt>. If an
     * error occurs, appends the error message to <tt>message</tt>.
     *
     * @param directory
     * @return
     */
    public void storeArtifacts(File directory) throws MavenException {
        File libDir = new File(directory.getAbsolutePath() + File.separator + DIR_LIB);
        if (!libDir.exists())
            libDir.mkdirs();
        Stream<Description> descriptions = Stream.empty();
        if (crDescription != null && crDescription.getMetaDescription() != null) {
            descriptions = Stream.concat(descriptions, Stream.of(crDescription));
        }
        if (cmDelegates != null)
            descriptions = Stream.concat(descriptions, cmDelegates.stream().filter(d -> Objects.nonNull(d.getMetaDescription())));
        if (aeDelegates != null)
            descriptions = Stream.concat(descriptions, aeDelegates.stream().filter(d -> !d.getMetaDescription().isPear() && Objects.nonNull(d.getMetaDescription())));
        if (ccDelegates != null)
            descriptions = Stream.concat(descriptions, ccDelegates.stream().filter(d -> Objects.nonNull(d.getMetaDescription())));
        storeArtifactsOfDescriptions(descriptions, libDir);

    }

    /**
     * Only called from {@link #storeArtifacts(File)}. Tries to find the artifact of the given description. In case the
     * artifact could not be found, retrieves the newest
     * version of the artifact in question and tries again. This can solve issues where a SNAPSHOT version was used
     * originally that is not available any more.
     *
     * @param descriptions The pipeline's component descriptions. Should be complete for conflict resolution.
     * @param libDir       The directory in which the dependencies should be stored.
     * @throws MavenException
     */
    private void storeArtifactsOfDescriptions(Stream<Description> descriptions, File libDir) throws MavenException {
        MavenConnector.storeArtifactsWithDependencies(descriptions.map(d -> d.getMetaDescription().getMavenArtifact()), libDir);
    }

    private void serializeDescriptions(File pipelineStorageDir, String targetFileName, Object descriptions) throws IOException {
        if (!pipelineStorageDir.exists())
            pipelineStorageDir.mkdirs();
        File targetFile = new File(pipelineStorageDir.getAbsolutePath() + File.separatorChar + targetFileName);
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(FileUtilities.getOutputStreamToFile(targetFile))) {
            objectOutputStream.writeObject(descriptions);
        }
    }

    private <T> T deserializeDescriptions(File pipelineStorageDir, String sourceFileName) throws IOException, ClassNotFoundException {
        File sourceFile = new File(pipelineStorageDir.getAbsolutePath() + File.separatorChar + sourceFileName);
        if (!sourceFile.exists())
            return null;
        try (ObjectInputStream objectInputStream = new ObjectInputStream(FileUtilities.getInputStreamFromFile(sourceFile))) {
            return (T) objectInputStream.readObject();
        }
    }

    public File getLoadDirectory() {
        return loadDirectory;
    }

    public void setLoadDirectory(File loadDirectory) {
        this.loadDirectory = loadDirectory;
    }

    /**
     * Loads the pipeline from the file system. The <tt>directory</tt> parameter must point to the root directory
     * of the pipeline directory structure. That is, it is expected that the given directory has two
     * subdirectory, {@value DIR_DESC} and {@value DIR_LIB} with the descriptor files and the library files,
     * respectively.
     *
     * @throws PipelineIOException If the pipeline cannot be loaded.
     */
    public JCoReUIMAPipeline load(boolean forEditing) throws PipelineIOException {
        if (loadDirectory == null)
            throw new IllegalStateException("The base directory for the pipeline has not been set, cannot load.");
        if (!loadDirectory.exists())
            throw new PipelineIOException("The JCoReUIMAPipeline directory "
                    + loadDirectory + " does not exist.");
        try {
            try {
                if (forEditing) {
                    crDescription = deserializeDescriptions(loadDirectory, SERIALIZED_CR_DESCS_FILE);
                    cmDelegates = deserializeDescriptions(loadDirectory, SERIALIZED_CM_DESCS_FILE);
                    aeDelegates = deserializeDescriptions(loadDirectory, SERIALIZED_AE_DESCS_FILE);
                    ccDelegates = deserializeDescriptions(loadDirectory, SERIALIZED_CC_DESCS_FILE);
                    // legacy support: early versions of the JCoReUIMAPipeline did not always have the lists
                    // instantiated so when loading old pipelines they could be null. Then, nothing can be added
                    // to these lists.
                    if (cmDelegates == null) cmDelegates = new ArrayList<>();
                    if (aeDelegates == null) aeDelegates = new ArrayList<>();
                    if (ccDelegates == null) ccDelegates = new ArrayList<>();
                }
            } catch (ClassNotFoundException e) {
                throw new PipelineIOException(e);
            }

            File descDir = new File(loadDirectory.getAbsolutePath() + File.separator + DIR_DESC);
            if (!descDir.exists())
                throw new PipelineIOException("The JCoReUIMAPipeline directory "
                        + loadDirectory + " does not have the descriptor sub directory " + DIR_DESC);
            File[] xmlFiles = descDir.listFiles(f -> f.getName().endsWith(".xml"));
            List<CollectionReaderDescription> crDescs = new ArrayList<>();
            List<AnalysisEngineDescription> cmDescs = new ArrayList<>();
            List<AnalysisEngineDescription> aaeCmDescs = new ArrayList<>();
            List<AnalysisEngineDescription> aeDescs = new ArrayList<>();
            List<AnalysisEngineDescription> aaeDescs = new ArrayList<>();
            List<ResourceSpecifier> ccDescs = new ArrayList<>();
            List<ResourceSpecifier> aaeCcDescs = new ArrayList<>();
            for (File xmlFile : xmlFiles) {
                // don't load the CPE AAE descriptor, it is solely needed when using the CPE descriptor on its own
                if (xmlFile.getName().equals(CPE_AAE_DESC_NAME))
                    continue;
                XMLParser parser = UIMAFramework.getXMLParser();
                ResourceSpecifier spec = null;
                try {
                    spec = parser.parseResourceSpecifier(
                            new XMLInputSource(xmlFile));
                } catch (InvalidXMLException e) {
                    if (log.isDebugEnabled()) {
                        List<String> messages = new ArrayList<>();
                        Throwable cause = e;
                        do {
                            messages.add(cause.getMessage());
                            cause = cause.getCause();
                        } while (cause != null);
                        log.debug("File {} could not be parsed as a UIMA component and is skipped: {}", xmlFile, messages.stream().collect(joining("; ")));
                    }
                }
                if (spec != null) {
                    spec.setSourceUrl(xmlFile.getAbsoluteFile().toURI().toURL());
                    if (spec instanceof CollectionReaderDescription) {
                        crDescs.add((CollectionReaderDescription) spec);
                    } else if (spec instanceof AnalysisEngineDescription) {
                        AnalysisEngineDescription aeDesc = (AnalysisEngineDescription) spec;
                        if (xmlFile.getName().toLowerCase().contains("consumer") ||
                                xmlFile.getName().toLowerCase().contains("writer")) {
                            log.debug("Adding the descriptor {} to CAS consumers because of its file name", xmlFile);
                            if (aeDesc.isPrimitive()) {
                                log.debug("Reading descriptor {} as CAS consumer", xmlFile);
                                ccDescs.add(spec);
                            } else {
                                aaeCcDescs.add(spec);
                            }
                        } else if (aeDesc.getAnalysisEngineMetaData().getOperationalProperties().getOutputsNewCASes()) {
                            log.debug("Reading descriptor {} as CAS multiplier", xmlFile);
                            if (aeDesc.isPrimitive())
                                cmDescs.add(aeDesc);
                            else
                                aaeCmDescs.add(aeDesc);
                        } else {
                            log.debug("Reading descriptor {} as analysis engine", xmlFile);
                            if (aeDesc.isPrimitive())
                                aeDescs.add(aeDesc);
                            else
                                aaeDescs.add(aeDesc);
                        }
                    } else if (spec instanceof CasConsumerDescription) {
                        ccDescs.add(spec);
                    } else {
                        log.debug("Ignoring file " + xmlFile + " because it is no UIMA component descriptor.");
                    }
                }
            }
            if (crDescs.isEmpty())
                log.warn("There is no CollectionReader descriptor in directory " + descDir.getAbsolutePath() + ". " +
                        "A pipeline without a CollectionReader cannot be run.");
            if (crDescs.size() > 1)
                throw new PipelineIOException("There are multiple CollectionReader descriptors in directory " +
                        descDir.getAbsolutePath() + ": " + crDescs.stream().map(ResourceSpecifier::getSourceUrlString).collect(joining("\n")));

            if (aeDescs.size() > 1 && aaeDescs.isEmpty())
                throw new PipelineIOException("There are multiple primitive Analysis Engine descriptors in directory " +
                        descDir.getAbsolutePath() + ": " + crDescs.stream().map(ResourceSpecifier::getSourceUrlString).collect(joining("\n")) +
                        ". While it would be possible to build an aggregate automatically, this cannot be done because the " +
                        "required order is unknown. Automatic ordering by annotator capabilities is currently not supported.");

            // the descriptions are loaded at the beginning of the method, if existent
            // When accessing aggregate engine delegates, their types are resolved. Thus, we first need to load
            // the libraries of the pipeline
            getClasspathElements().forEach(JarLoader::addJarToClassPath);
            if (crDescription == null && crDescs != null && !crDescs.isEmpty())
                crDescription = new Description(crDescs.get(0).getSourceUrl());
            if (!cmDescs.isEmpty())
                aaeCmDesc = cmDescs.get(0);
            if (aaeCmDescs.size() == 1)
                aaeCmDesc = aaeCmDescs.get(0);
            if (aaeCmDescs.size() > 1)
                setAaeDesc(descDir, cmDescs, aaeCmDescs, "CAS multiplier", desc -> aaeCmDesc = desc);
            if (!aeDescs.isEmpty())
                aaeDesc = aeDescs.get(0);
            if (aaeDescs.size() == 1)
                aaeDesc = aaeDescs.get(0);
            if (aaeDescs.size() > 1)
                setAaeDesc(descDir, aeDescs, aaeDescs, "analysis engine", desc -> aaeDesc = desc);
            if (!ccDescs.isEmpty())
                ccDesc = (ResourceCreationSpecifier) ccDescs.get(0);
            if (aaeCcDescs.size() == 1)
                ccDesc = (ResourceCreationSpecifier) aaeCcDescs.get(0);
            if (aaeCcDescs.size() > 1) {
                // This should work because the pipeline should only store valid configurations. If there are multiple
                // consumers, they must be AAEs
                List<AnalysisEngineDescription> ccAeDescs = ccDescs.stream().map(AnalysisEngineDescription.class::cast).collect(toList());
                List<AnalysisEngineDescription> ccAaeDescs = aaeCcDescs.stream().map(AnalysisEngineDescription.class::cast).collect(toList());
                setAaeDesc(descDir, ccAeDescs, ccAaeDescs, "CAS consumer", desc -> ccDesc = desc);
            }

            // Set the descriptors to the descriptions as they were read from file. The descriptions have their own version
            // which in principle is fine. But since we store the descriptions in a binary format, they cannot easily be
            // modified outside of the pipeline builder. But we want to make traditional editing possible.
            if (crDescription != null && !crDescs.isEmpty())
                crDescription.setDescriptor(crDescs.get(0));
            if (aaeCmDesc != null && !cmDelegates.isEmpty())
                setAaeDescriptors(aaeCmDesc, cmDelegates, "CAS Multiplier");
            if (aaeDesc != null && !aeDelegates.isEmpty())
                setAaeDescriptors(aaeDesc, aeDelegates, "Analysis Engine");
            if (ccDesc != null && !ccDelegates.isEmpty() && ccDesc instanceof AnalysisEngineDescription)
                setAaeDescriptors((AnalysisEngineDescription) ccDesc, ccDelegates, "CAS Consumer");


            File confDir = new File(loadDirectory.getAbsolutePath() + File.separator + DIR_CONF);
            if (confDir.exists()) {
                File[] files = confDir.listFiles((dir, name) -> !name.equals(JAR_CONF_FILES));
                File configJar = new File(confDir.getAbsolutePath() + File.separator + JAR_CONF_FILES);
                log.debug("Packaging configuration data files {} into the JAR file {} to be able to load it for " +
                        "the pipeline runner.", files, configJar);
                FileUtilities.createJarFile(configJar, files);
            }


        } catch (IOException | InvalidXMLException | URISyntaxException e) {
            throw new PipelineIOException(e);
        }
        return this;
    }

    /**
     * Used to set the UIMA descriptors, that have been loaded from files in the desc/ directory, into their
     * deserialized {@link Description} instances. The idea is that in this way, editing the component parameters
     * can be done with the pipeline builder but also manually for quick changes.
     *
     * @param aae
     * @param descriptions
     * @param type
     * @throws PipelineIOException
     * @throws InvalidXMLException
     */
    private void setAaeDescriptors(AnalysisEngineDescription aae, List<Description> descriptions, String type) throws PipelineIOException, InvalidXMLException {
        if (!aae.isPrimitive()) {
            FlowConstraints flowConstraints = aae.getAnalysisEngineMetaData().getFlowConstraints();
            if (!(flowConstraints instanceof FixedFlow))
                throw new PipelineIOException(String.format("The %s aggregate does not define a FixedFlow. Only FixedFlow constraints are currently supported.", type));
            FixedFlow flow = (FixedFlow) flowConstraints;
            for (int i = 0; i < flow.getFixedFlow().length; ++i) {
                String component = flow.getFixedFlow()[i];
                ResourceSpecifier descriptor = aae.getDelegateAnalysisEngineSpecifiers().get(component);
                if (i < descriptions.size())
                    descriptions.get(i).setDescriptor(descriptor);
            }
            if (flow.getFixedFlow().length != descriptions.size()) {
                log.error("The fixed flow of the AAE with name {} is of length {} but there are {} descriptions available. Shortening the AAE. You need to check if the result is usable for you.", aae.getMetaData().getName(), flow.getFixedFlow().length, descriptions.size());
                int newlength = Math.min(flow.getFixedFlow().length, descriptions.size());
                List<Description> newDescList = descriptions.subList(0, newlength);
                String[] newflow = newDescList.stream().map(desc -> desc.getDescriptorAsAnalysisEngineDescription().getMetaData().getName()).toArray(String[]::new);
                flow.setFixedFlow(newflow);
            }
            log.debug("For the {} aggregate, the following delegate descriptors were set: {}", type, Stream.of(flow.getFixedFlow()).collect(joining(", ")));
        } else {
            if (descriptions.size() > 1)
                log.error("The {} is not an aggregate but there are {} descriptions with the following names: {}", type, descriptions.size(), descriptions.stream().map(Description::getName).collect(joining(", ")));
            descriptions.get(0).setDescriptor(aae);
        }
    }

    /**
     * Expects that the given AEs and AAEs form a delegation-tree, throws a PipelineIOException otherwise. Sets
     * the top AAE descriptor - that is the one that is not the delegate of any other AAE - via the
     * <tt>descSetter</tt> consumer.
     *
     * @param descDir
     * @param aeDescs
     * @param aaeDescs
     * @param componentType
     * @param descSetter
     * @throws InvalidXMLException
     * @throws PipelineIOException
     */
    private void setAaeDesc(File descDir, List<AnalysisEngineDescription> aeDescs, List<AnalysisEngineDescription> aaeDescs, String componentType, Consumer<AnalysisEngineDescription> descSetter) throws InvalidXMLException, PipelineIOException {
        Set<String> aeUris = aeDescs.stream().map(AnalysisEngineDescription::getSourceUrl).map(URL::toString).collect(toSet());
        Set<String> aaeUris = aaeDescs.stream().map(AnalysisEngineDescription::getSourceUrl).map(URL::toString).collect(toSet());
        Set<String> delegateUris = new HashSet<>();
        for (AnalysisEngineDescription aaeDesc : aaeDescs) {
            delegateUris.addAll(aaeDesc.getDelegateAnalysisEngineSpecifiers().values().stream().map(ResourceSpecifier::getSourceUrl).map(URL::toString).collect(toList()));
        }
        Sets.SetView<String> topAAEs = Sets.difference(aaeUris, delegateUris);
        Sets.SetView<String> aesNotInAAE = Sets.difference(aeUris, delegateUris);

        if (topAAEs.size() > 1)
            throw new PipelineIOException("There are multiple " + componentType + "s in " + descDir.getAbsolutePath()
                    + " that don't have a common super AAE. The pipeline cannot be built because it is unknown which to use.");

        if (!topAAEs.isEmpty()) {
            String topAAEUri = topAAEs.iterator().next();
            descSetter.accept(aaeDescs.stream().filter(aaeDesc -> aaeDesc.getSourceUrlString().equals(topAAEUri)).findFirst().get());
            if (!aesNotInAAE.isEmpty())
                log.warn("The AAE {} is used for the " + componentType + " + part of the pipeline. Primitive Analysis Engines " +
                        "that are no direct or indirect delegate of this AAE will not be run. The following " + componentType + "s are " +
                        "not a part of the AAE: {}", topAAEUri, aesNotInAAE);
        } else {
            descSetter.accept(aaeDescs.get(0));
        }
    }

    /**
     * Returns all analysis engines that have been added to the pipeline. These also include CAS multipliers which
     * are a variant of analysis engines with the "outputs new CASes" operational parameter set to <tt>true</tt>.
     * When storing the pipeline, all these
     * engines are bundled into a single aggregate analysis engine.
     *
     * @return The analysis engines of this pipeline, including multipliers.
     */
    public List<Description> getAeDelegates() {
        return aeDelegates;
    }

    /**
     * Add an analysis engine to this pipeline. The order is important: The engines are stored in a list that
     * directly determines the order in which the components are called during processing.
     *
     * @param aeDesc The descriptor of an analysis engine.
     */
    public void addDelegateAe(Description aeDesc) {
        if (!aeDesc.getMetaDescription().isPear()) {
            if (!(aeDesc.getDescriptor() instanceof AnalysisEngineDescription))
                throw new IllegalArgumentException("The passed description " + aeDesc + " does not specify an analysis " +
                        "engine description but an instance of " + aeDesc.getDescriptor().getClass().getCanonicalName());
            if (aeDesc.getDescriptorAsAnalysisEngineDescription().getAnalysisEngineMetaData().getOperationalProperties().getOutputsNewCASes())
                throw new IllegalArgumentException("The passed description " + aeDesc + " is set to output new CASes, i.e. " +
                        "it is a CAS multiplier. Add it via the appropriate method to the pipeline.");
        }
        aeDelegates.add(aeDesc);
    }

    /**
     * Add a CAS multiplier to this pipeline. The order is important: The multipliers are stored in a list that
     * directly determines the order in which the components are called during processing. The CAS multiplier(s) will
     * be arranged directly after the Collection Reader and before the analysis engines.
     *
     * @param multiplier The descriptor of a CAS multiplier.
     */
    public void addCasMultiplier(Description multiplier) {
        if (!(multiplier.getDescriptor() instanceof AnalysisEngineDescription))
            throw new IllegalArgumentException("The passed description " + multiplier + " does not specify an analysis " +
                    "engine description but an instance of " + multiplier.getDescriptor().getClass().getCanonicalName());
        if (!multiplier.getDescriptorAsAnalysisEngineDescription().getAnalysisEngineMetaData().getOperationalProperties().getOutputsNewCASes())
            throw new IllegalArgumentException("The passed description " + multiplier + " is set not to output new CASes, i.e. " +
                    "it is a simple analysis engine and not a CAS multiplier. Add it via the appropriate method to the pipeline.");
        cmDelegates.add(multiplier);
    }

    public Stream<File> getClasspathElements() throws PipelineIOException {
        File libDir = null;
        if (loadDirectory != null)
            libDir = new File(loadDirectory.getAbsolutePath() + File.separator + DIR_LIB);
        if (loadDirectory != null && (!libDir.exists() || libDir.list().length == 0)) {
            try {
                storeArtifacts(loadDirectory);
            } catch (MavenException e) {
                throw new PipelineIOException("Error occurred when trying to store the Maven artifacts to the " + loadDirectory.getAbsolutePath() + File.separator + DIR_LIB + " directory. This storage is necessary to return the classpath elements which was requested by calling this method.", e);
            }
        }
        if (loadDirectory != null && libDir.exists()) {
            File configJar = new File(loadDirectory.getAbsolutePath() + File.separator + DIR_CONF);

            File[] libFiles = libDir.listFiles((dir, name) -> name.endsWith(".jar") || name.endsWith(".zip"));

            Stream<File> libFilesStream = Stream.of(libFiles);
            if (configJar.exists())
                libFilesStream = Stream.concat(libFilesStream, Stream.of(configJar));
            return libFilesStream;
        } else {
            List<Stream<MavenArtifact>> artifactList = new ArrayList<>();
            Function<Description, MavenArtifact> artifactExtractor = d ->
                    d.getMetaDescription() != null ? d.getMetaDescription().getMavenArtifact() : null;
            if (crDescription != null)
                artifactList.add(Stream.of(artifactExtractor.apply(crDescription)));
            if (cmDelegates != null)
                artifactList.add(cmDelegates.stream().map(artifactExtractor));
            if (aeDelegates != null)
                artifactList.add(aeDelegates.stream().map(artifactExtractor));
            if (ccDelegates != null)
                artifactList.add(ccDelegates.stream().map(artifactExtractor));
            // We filter for null objects because PEAR components don't have a Maven artifact
            return artifactList.stream().flatMap(Function.identity()).filter(Objects::nonNull).flatMap(artifact -> {
                try {
                    return MavenConnector.getDependencies(artifact);
                } catch (MavenException e) {
                    log.error("Maven exception while trying to get transitive dependencies of artifact {}:{}:{}",
                            artifact.getGroupId(), artifact.getArtifactId(), artifact.getVersion(), e);
                }
                return null;
            }).map(Artifact::getFile);
        }
    }

    /**
     * Removes the given description and the maven artifacts associated with the component.
     *
     * @param description The description of the component to be removed.
     */
    public void removeComponent(Description description) {
        if (description.equals(crDescription)) {
            crDescription = null;
        }
        if (aeDelegates != null && aeDelegates.contains(description)) {
            aeDelegates.remove(description);
            aaeDesc = null;
        }
        if (cmDelegates != null && cmDelegates.contains(description)) {
            cmDelegates.remove(description);
            aaeCmDesc = null;
        }
        if (ccDelegates != null && ccDelegates.contains(description)) {
            ccDelegates.remove(description);
            ccDesc = null;
        }
        // We keep track components removed from the pipeline. If the respective changes are stored to disc,
        // the respective descriptor files ought to be removed.
        if (description.getUimaDescPath() != null)
            filesToDeleteOnSave.add(description.getUimaDescPath());
    }

    public String getClassPath() throws PipelineIOException {
        Stream<File> artifactFiles = getClasspathElements();
        Stream<String> classpathElements = artifactFiles.map(File::getAbsolutePath);
        return classpathElements.collect(joining(File.pathSeparator));
    }

    public AnalysisEngineDescription getAaeCmDesc() {
        return aaeCmDesc;
    }

    /**
     * Returns the CAS consumer of this pipeline or null, if none is set. The return value is either an
     * {@link AnalysisEngineDescription} or a {@link CasConsumerDescription}. If an AnalysisEngineDescription is
     * returned, it might be an aggregate.
     *
     * @return The consumer description or null, if none is set.
     */
    public ResourceCreationSpecifier getCcDesc() {
        return ccDesc;
    }

    public List<Description> getCmDelegates() {
        return cmDelegates;
    }

    /**
     * Returns an aggregate including CM, AE and CC.
     *
     * @return An aggregate including CM, AE and CC.
     */
    public AnalysisEngineDescription getCompleteAggregateDescription() throws ResourceInitializationException {
        return AnalysisEngineFactory.createEngineDescription(Stream.of(aaeCmDesc, aaeDesc, (AnalysisEngineDescription) ccDesc).filter(Objects::nonNull).toArray(AnalysisEngineDescription[]::new));
    }

    public void clear() {
        crDescription = null;
        if (aeDelegates != null)
            aeDelegates.clear();
        if (cmDelegates != null)
            cmDelegates.clear();
        aaeDesc = null;
        aaeCmDesc = null;
        ccDesc = null;
        if (ccDelegates != null)
            ccDelegates.clear();

        loadDirectory = null;
    }
}
