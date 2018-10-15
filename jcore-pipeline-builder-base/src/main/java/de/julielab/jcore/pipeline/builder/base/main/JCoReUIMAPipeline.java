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
import org.apache.uima.analysis_engine.metadata.FixedFlow;
import org.apache.uima.analysis_engine.metadata.FlowConstraints;
import org.apache.uima.analysis_engine.metadata.impl.FixedFlow_impl;
import org.apache.uima.collection.CasConsumer;
import org.apache.uima.collection.CasConsumerDescription;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.collection.impl.metadata.cpe.CpeCasProcessorsImpl;
import org.apache.uima.collection.impl.metadata.cpe.CpeComponentDescriptorImpl;
import org.apache.uima.collection.impl.metadata.cpe.CpeIncludeImpl;
import org.apache.uima.collection.impl.metadata.cpe.CpeIntegratedCasProcessorImpl;
import org.apache.uima.collection.metadata.CpeDescription;
import org.apache.uima.collection.metadata.CpeDescriptorException;
import org.apache.uima.fit.cpe.CpeBuilder;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.resource.ResourceCreationSpecifier;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.MetaDataObject;
import org.apache.uima.resource.metadata.ResourceMetaData;
import org.apache.uima.resource.metadata.impl.Import_impl;
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
import java.util.stream.Collectors;
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

        String message = "";
        if ((aaeDesc == null && (aeDelegates == null || aeDelegates.isEmpty()) && ccDesc == null && (ccDelegates == null || ccDelegates.isEmpty())) || crDescription == null) {
            message = "This pipeline has either no collection reader or no analysis engines and no consumer. " +
                    "A reader and an analysis engine or a consumer is required to do any work. The pipeline will be stored anyway but will need additional work.";
            log.warn(message);
        }

        // Load the libraries for this pipeline. They are required for aggregate engine creation.
        getClasspathElements().forEach(JarLoader::addJarToClassPath);

        // Store descriptions with their meta data
        try {
            serializeDescriptions(directory, SERIALIZED_CR_DESCS_FILE, crDescription);
            serializeDescriptions(directory, SERIALIZED_CM_DESCS_FILE, cmDelegates);
            serializeDescriptions(directory, SERIALIZED_AE_DESCS_FILE, aeDelegates);
            serializeDescriptions(directory, SERIALIZED_CC_DESCS_FILE, ccDelegates);
        } catch (IOException e) {
            throw new PipelineIOException(e);
        }

        // Store descriptors
        try {
            if (aeDelegates != null) {

                Stream<AnalysisEngineDescription> descStream = aeDelegates.stream().
                        filter(desc -> !desc.getMetaDescription().isPear()).
                        map(Description::getDescriptorAsAnalysisEngineDescription);
                aaeDesc = AnalysisEngineFactory.createEngineDescription(descStream.toArray(AnalysisEngineDescription[]::new));
                Map<String, MetaDataObject> delegatesWithImports = aaeDesc.getDelegateAnalysisEngineSpecifiersWithImports();
                LinkedHashMap<String, MetaDataObject> delegatesWithImportsCopy = new LinkedHashMap<>(delegatesWithImports);
                Iterator<Map.Entry<String, MetaDataObject>> delegateIt = delegatesWithImportsCopy.entrySet().iterator();
                delegatesWithImports.clear();
                List<String> flowNames = new ArrayList<>();
                for (int i = 0; i < aeDelegates.size(); ++i) {
                    Description description = aeDelegates.get(i);
                    if (!description.getMetaDescription().isPear()) {
                        Map.Entry<String, MetaDataObject> delegate = delegateIt.next();
                        delegatesWithImports.put(delegate.getKey(), delegate.getValue());
                        flowNames.add(delegate.getKey());
                    } else {
                        Import_impl imp = new Import_impl();
                        File pearDescriptorFile = new File(description.getLocation());
                        imp.setName(description.getName());
                        imp.setLocation(pearDescriptorFile.toURI().toString());
                        imp.setSourceUrl(pearDescriptorFile.toURI().toURL());
                        delegatesWithImports.put(description.getName(), imp);
                        flowNames.add(description.getName());
                    }
                }
                assert !delegateIt.hasNext();
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
            File descDir = new File(directory.getAbsolutePath() + File.separator + DIR_DESC);
            if (!descDir.exists())
                descDir.mkdirs();
            File crFile = null;
            if (crDescription != null) {
                String pathname = descDir.getAbsolutePath() +
                        File.separator + crDescription.getName() + ".xml";
                crFile = new File(pathname);
                crDescription.getDescriptor().setSourceUrl(crFile.toURI().toURL());
                crDescription.getDescriptor().toXML(FileUtilities.getWriterToFile(
                        crFile));
            }
            File cmFile = null;
            if (cmDelegates != null && cmDelegates.size() == 1) {
                Description cm = cmDelegates.get(0);
                aaeCmDesc = cm.getDescriptorAsAnalysisEngineDescription();
                String pathname = descDir.getAbsolutePath() +
                        File.separator + cm.getName() + ".xml";
                cmFile = new File(pathname);
                cm.getDescriptor().setSourceUrl(cmFile.toURI().toURL());
                cm.getDescriptor().toXML(FileUtilities.getWriterToFile(
                        cmFile));
            } else if (cmDelegates != null && cmDelegates.size() > 1) {
                cmFile = new File(descDir.getAbsolutePath() +
                        File.separator +
                        "AggregateMultiplier.xml");
                aaeCmDesc.setSourceUrl(cmFile.toURI().toURL());
                aaeCmDesc.getMetaData().setName("JCoRe Multiplier AAE");
                aaeCmDesc.getMetaData().setDescription("This AAE descriptor directly contains the CAS multipliers added " +
                        "through the JCoRe pipeline builder. The AAE serves to bundle all the components together.");
                aaeCmDesc.toXML(FileUtilities.getWriterToFile(
                        cmFile));
            }

            File aaeFile = null;
            if (aaeDesc != null) {
                String aeName = aaeDesc.getMetaData().getName();
                if (aeName == null)
                    aeName = "aggregateAnalysisEngine";
                aaeFile = new File(descDir.getAbsolutePath() +
                        File.separator +
                        aeName + ".xml");
                aaeDesc.setSourceUrl(aaeFile.toURI().toURL());
                aaeDesc.getMetaData().setName("JCoRe Pipeline AAE");
                aaeDesc.getMetaData().setDescription("This AAE descriptor directly contains the analysis engines added " +
                        "through the JCoRe pipeline builder. The AAE serves to bundle all the components together.");
                aaeDesc.toXML(FileUtilities.getWriterToFile(
                        aaeFile));
            }

            File ccFile = null;
            if (ccDelegates != null && !ccDelegates.isEmpty()) {
                for (Description ccDesc : ccDelegates)
                    storeCCDescriptor(ccDesc, descDir);
                if (ccDelegates.size() > 1) {
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
                    ccFile = new File(descDir.getAbsolutePath() +
                            File.separator +
                            "AggregateConsumer.xml");
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
                if (ccDelegates == null || ccDelegates.stream().map(Description::getDescriptor).filter(CasConsumer.class::isInstance).count() == 0) {
                    CpeBuilder builder = new CpeBuilder();
                    if (crDescription != null)
                        builder.setReader(crDescription.getDescriptorAsCollectionReaderDescription());
                    AnalysisEngineDescription cpeAAE = AnalysisEngineFactory.createEngineDescription();
                    cpeAAE.getDelegateAnalysisEngineSpecifiersWithImports().clear();
                    if (cmFile != null) {
                        Import_impl cmImport = new Import_impl();
                        cmImport.setLocation(cmFile.getName());
                        cmImport.setSourceUrl(cmFile.toURI().toURL());
                        cpeAAE.getDelegateAnalysisEngineSpecifiersWithImports().put(aaeCmDesc.getMetaData().getName(), cmImport);
                    }
                    if (aaeFile != null) {
                        Import_impl aaeImport = new Import_impl();
                        aaeImport.setLocation(aaeFile.getName());
                        aaeImport.setSourceUrl(aaeFile.toURI().toURL());
                        cpeAAE.getDelegateAnalysisEngineSpecifiersWithImports().put(aaeDesc.getMetaData().getName(), aaeImport);
                    }
                    if (ccFile != null) {
                        Import_impl ccImport = new Import_impl();
                        ccImport.setLocation(ccFile.getName());
                        ccImport.setSourceUrl(ccFile.toURI().toURL());
                        cpeAAE.getDelegateAnalysisEngineSpecifiersWithImports().put(ccDesc.getMetaData().getName(), ccImport);
                    }
                    String[] flow = Stream.of(aaeCmDesc, aaeDesc, ccDesc).filter(Objects::nonNull).map(ResourceCreationSpecifier::getMetaData).map(ResourceMetaData::getName).toArray(String[]::new);
                    ((FixedFlow) cpeAAE.getAnalysisEngineMetaData().getFlowConstraints()).setFixedFlow(flow);
                    File cpeAAEFile = new File(descDir.getAbsolutePath() + File.separator + CPE_AAE_DESC_NAME);
                    cpeAAE.toXML(
                            FileUtilities.getWriterToFile(
                                    cpeAAEFile));
                    builder.setAnalysisEngine(cpeAAE);
                    CpeDescription cpeDescription = builder.getCpeDescription();
                    if (crDescription != null)
                        cpeDescription.getAllCollectionCollectionReaders()[0].getCollectionIterator().
                                getDescriptor().getInclude().set(crFile.getName());

                    CpeIncludeImpl cpeInclude = new CpeIncludeImpl();
                    cpeInclude.set(cpeAAEFile.getName());
                    CpeComponentDescriptorImpl cpeComponentDescriptor = new CpeComponentDescriptorImpl();
                    cpeComponentDescriptor.setInclude(cpeInclude);
                    CpeIntegratedCasProcessorImpl cpeIntegratedCasProcessor = new CpeIntegratedCasProcessorImpl();
                    cpeIntegratedCasProcessor.setCpeComponentDescriptor(cpeComponentDescriptor);
                    cpeIntegratedCasProcessor.setName("CPE AAE");
                    CpeCasProcessorsImpl cpeCasProcessors = new CpeCasProcessorsImpl();
                    cpeCasProcessors.addCpeCasProcessor(cpeIntegratedCasProcessor);
                    cpeDescription.setCpeCasProcessors(cpeCasProcessors);
                    cpeDescription.toXML(FileUtilities.getWriterToFile(
                            new File(descDir.getAbsolutePath() + File.separator +
                                    "CPE.xml")
                    ));
                } else
                    log.warn("Could not store a CPE descriptor because a CasConsumer is included in the pipeline that " +
                            "implements a CasConsumer interface rather than the AnalysisEngine interface. Note " +
                            "that CasConsumers are basically analysis engines since UIMA 2.0 and that there is " +
                            "no downside in using AEs as consumers.");
            } catch (InvalidXMLException e) {
                log.error("Could not store the CPE descriptor: " + e);
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
                FileUtils.write(new File(directory.getAbsolutePath() + File.separator + "pipelinebuilderversion.txt"), version + System.getProperty("line.separator"), StandardCharsets.UTF_8.name());
            } catch (IOException e) {
                log.warn("Could not write pipeline builder version to file:", e);
            }
        }

        try {
            storeArtifacts(directory);
        } catch (MavenException e) {
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
        ccFile = new File(descDir.getAbsolutePath() +
                File.separator +
                ccDesc.getName() + ".xml");
        ccDesc.getDescriptor().toXML(FileUtilities.getWriterToFile(ccFile));
        ccDesc.setUri(ccFile.toURI());
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
        if (crDescription != null && crDescription.getMetaDescription() != null) {
            storeArtifactOfDescription(crDescription, libDir);
        }
        if (cmDelegates != null)
            for (Description d : cmDelegates)
                if (d.getMetaDescription() != null)
                    storeArtifactOfDescription(d, libDir);
        if (aeDelegates != null)
            for (Description d : aeDelegates)
                if (!d.getMetaDescription().isPear() && d.getMetaDescription() != null)
                    storeArtifactOfDescription(d, libDir);
        if (ccDelegates != null)
            for (Description d : ccDelegates) {
                if (d.getMetaDescription() != null)
                    storeArtifactOfDescription(d, libDir);
            }
    }

    /**
     * Only called from {@link #storeArtifacts(File)}. Tries to find the artifact of the given description. In case the
     * artifact could not be found, retrieves the newest
     * version of the artifact in question and tries again. This can solve issues where a SNAPSHOT version was used
     * originally that is not available any more.
     *
     * @param description
     * @param libDir
     * @throws MavenException
     */
    private void storeArtifactOfDescription(Description description, File libDir) throws MavenException {
        try {
            MavenConnector.storeArtifactWithDependencies(description.getMetaDescription().getMavenArtifact(), libDir);
        } catch (MavenException e) {
            log.error("Could not receive artifact {}. Trying to find any available version of the artifact and setting it to the newest version.", description.getMetaDescription().getMavenArtifact());
            String newestVersion = MavenConnector.getNewestVersion(description.getMetaDescription().getMavenArtifact());
            description.getMetaDescription().getMavenArtifact().setVersion(newestVersion);
            MavenConnector.storeArtifactWithDependencies(description.getMetaDescription().getMavenArtifact(), libDir);
        }
    }

    private void storeDelegateDescriptors(File directory, List<Description> descriptors, String delegatesDir) throws SAXException, IOException {
        if (descriptors != null) {
            File aeDelegateDir = new File(directory.getAbsolutePath() + File.separatorChar +
                    delegatesDir);
            if (!aeDelegateDir.exists())
                aeDelegateDir.mkdirs();
            for (Description aeDesc : descriptors) {
                File aeDelegateStorageFile = new File(aeDelegateDir.getAbsolutePath() + File.separatorChar
                        + aeDesc.getName() + ".xml");
                aeDesc.getDescriptor().toXML(FileUtilities.getWriterToFile(aeDelegateStorageFile));
            }
        }
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
                        if (aeDesc.isPrimitive()
                                && (xmlFile.getName().toLowerCase().contains("consumer") ||
                                xmlFile.getName().toLowerCase().contains("writer"))) {
                            log.debug("Adding the descriptor {} to CAS consumers because of its file name", xmlFile);
                            if (aeDesc.isPrimitive()) {
                                log.debug("Reading descriptor {} as CAS consumer", xmlFile);
                                ccDescs.add(spec);
                            }
                            else {
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
        String fmt = "The %s aggregate does not define a FixedFlow. Only FixedFlow constraints are currently supported.";
        if (!aae.isPrimitive()) {
            FlowConstraints flowConstraints = aae.getAnalysisEngineMetaData().getFlowConstraints();
            if (!(flowConstraints instanceof FixedFlow))
                throw new PipelineIOException(String.format(fmt, type));
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
