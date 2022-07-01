package de.julielab.jcore.pipeline.builder.base.main;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import de.julielab.java.utilities.FileUtilities;
import de.julielab.java.utilities.classpath.JarLoader;
import de.julielab.jcore.pipeline.builder.base.PipelineParameterChecker;
import de.julielab.jcore.pipeline.builder.base.exceptions.PipelineIOException;
import de.julielab.utilities.aether.AetherUtilities;
import de.julielab.utilities.aether.MavenArtifact;
import de.julielab.utilities.aether.MavenException;
import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.impl.AnalysisEngineDescription_impl;
import org.apache.uima.analysis_engine.metadata.AnalysisEngineMetaData;
import org.apache.uima.analysis_engine.metadata.FixedFlow;
import org.apache.uima.analysis_engine.metadata.FlowConstraints;
import org.apache.uima.analysis_engine.metadata.impl.FixedFlow_impl;
import org.apache.uima.collection.CasConsumer;
import org.apache.uima.collection.CasConsumerDescription;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.collection.base_cpm.CasProcessor;
import org.apache.uima.collection.impl.metadata.cpe.CpeCasProcessorsImpl;
import org.apache.uima.collection.metadata.CpeCasProcessor;
import org.apache.uima.collection.metadata.CpeDescription;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.flow.FlowControllerDescription;
import org.apache.uima.resource.ResourceCreationSpecifier;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.Import;
import org.apache.uima.resource.metadata.MetaDataObject;
import org.apache.uima.resource.metadata.ResourceMetaData;
import org.apache.uima.resource.metadata.TypeSystemDescription;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.*;

public class JCoReUIMAPipeline {
    public static final String DIR_DESC = "desc";
    public static final String DIR_DESC_ALL = "descAll";
    public static final String DIR_LIB = "lib";
    public static final String DIR_CONF = "config";
    public static final String CPE_AAE_DESC_NAME = "cpeAAE.xml";
    public static final String AGGREGATE_ANALYSIS_ENGINE_WITH_INTEGRATED_DELEGATE_DESCRIPTORS_XML = "AggregateAnalysisEngineWithIntegratedDelegateDescriptors.xml";
    private static final String SERIALIZED_CR_DESCS_FILE = "crDescriptions.json";
    private static final String SERIALIZED_CM_DESCS_FILE = "cmDescriptions.json";
    private static final String SERIALIZED_AE_FLOW_CONTROLLER_DESCS_FILE = "aeFlowControllerDescriptions.json";
    private static final String SERIALIZED_AE_DESCS_FILE = "aeDescriptions.json";
    private static final String SERIALIZED_CC_FLOW_CONTROLLER_DESCS_FILE = "ccFlowControllerDescriptions.json";
    private static final String SERIALIZED_CC_DESCS_FILE = "ccDescriptions.json";
    private final static Logger log = LoggerFactory.getLogger(JCoReUIMAPipeline.class);
    private static final Function<List<Description>, Stream<Import>> tsImportsExtractor = descs -> descs.stream().flatMap(desc -> {
        final AnalysisEngineMetaData analysisEngineMetaData = desc.getDescriptorAsAnalysisEngineDescription().getAnalysisEngineMetaData();
        if (analysisEngineMetaData == null)
            return null;
        final TypeSystemDescription typeSystem = analysisEngineMetaData.getTypeSystem();
        if (typeSystem == null)
            return null;
        return Stream.of(typeSystem.getImports());
    }).filter(Objects::nonNull);
    /**
     * We keep track components removed from the pipeline. If the respective changes are stored to disc,
     * the respective descriptor files ought to be removed.
     */
    private final Set<String> filesToDeleteOnSave = new HashSet<>();
    /**
     * The parent POM is used for dependency resolution of the component artifacts. It can be used to resolve
     * library version conflicts using the dependencyManagement mechanism. May be <tt>null</tt>.
     */
    private MavenArtifact parentPom;
    private Description crDescription;
    private List<Description> aeDelegates;
    private List<Description> cmDelegates;
    private List<Description> ccDelegates;
    /**
     * The flow controller set to the AE aggregate, if not null.
     */
    private Description aeFlowController;
    /**
     * The flow controller set to the CC aggregate, if not null.
     */
    private Description ccFlowController;
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
    private boolean areLibrariesLoaded;

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

    public MavenArtifact getParentPom() {
        return parentPom;
    }

    public void setParentPom(MavenArtifact parentPom) {
        this.parentPom = parentPom;
        if ((parentPom.getGroupId() == null || parentPom.getArtifactId() == null || parentPom.getVersion() == null) && parentPom.getFile() != null)
            parentPom.setCoordinatesFromFile();
    }

    /**
     * @return Returns the multipliers from within the aeDelegates.
     */
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
        avoidNamingCollisions(crDescription);
        this.crDescription = crDescription;
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
        if (ccDelegates.stream().map(Description::getDescriptor).anyMatch(CasConsumerDescription.class::isInstance))
            throw new IllegalArgumentException("There is already a consumer represented by a " +
                    " " + CasConsumerDescription.class.getCanonicalName() + ". " +
                    "Those are deprecated and only one can be used in each pipeline.");
        avoidArtifactVersionConflicts(ccDesc);

        avoidNamingCollisions(ccDesc);
        this.ccDelegates.add(ccDesc);
    }

    public void store(File directory) throws PipelineIOException {
        store(directory, false);
    }

    public void store(File directory, boolean populateLibDir) throws PipelineIOException {

        String message = "";
        if ((aaeDesc == null && (aeDelegates == null || aeDelegates.isEmpty()) && ccDesc == null && (ccDelegates == null || ccDelegates.isEmpty())) || crDescription == null) {
            message = "This pipeline has either no collection reader or no analysis engines and no consumer. " +
                    "A reader and an analysis engine or a consumer is required to do any work. The pipeline will be stored anyway but will need additional work.";
            log.warn(message);
        }

        // Load the libraries for this pipeline. They are required for aggregate engine creation.
        //getClasspathElements().forEach(JarLoader::addJarToClassPath);

        // Store descriptors
        try {
            File descDir = new File(directory.getAbsolutePath() + File.separator + DIR_DESC);
            File descDirAll = new File(directory.getAbsolutePath() + File.separator + DIR_DESC_ALL);
            if (!descDir.exists()) {
                descDir.mkdirs();
            } else {
                // Clear the desc directory to avoid leftover descriptors which can cause issues when loading
                Stream.of(descDir.listFiles()).forEach(File::delete);
            }
            storeAllDescriptors(descDirAll);
            if (aeDelegates.stream().anyMatch(Description::isActive)) {
                Stream<AnalysisEngineDescription> descStream = aeDelegates.stream().
                        // filter(desc -> !desc.getMetaDescription().isPear()).
                                filter(Description::isActive).
                        map(Description::getDescriptorAsAnalysisEngineDescription);
                aaeDesc = createAAEWithImportedDelegates(descDir, "AggregateAnalysisEngine", aeDelegates, descStream, true, aeFlowController);
            } else {
                aaeDesc = null;
            }
            // Create the CM AAE, if required
            if (cmDelegates != null && cmDelegates.stream().filter(Description::isActive).count() > 1) {
                Stream<AnalysisEngineDescription> descStream = cmDelegates.stream().filter(Description::isActive).
                        map(Description::getDescriptorAsAnalysisEngineDescription);
                aaeCmDesc = createAAEWithImportedDelegates(descDir, "AggregateMultiplier", cmDelegates, descStream, true, null);
            }
            File crFile;
            if (crDescription != null) {
                crFile = getDescriptorStoragePath(crDescription, descDir).toFile();

                crDescription.getDescriptor().setSourceUrl(crFile.toURI().toURL());
                crDescription.getDescriptor().toXML(FileUtilities.getWriterToFile(
                        crFile));
                crDescription.setUimaDescPath(crFile.getName());
                filesToDeleteOnSave.remove(crFile.getName());
            }
            File cmFile = new File(descDir.getAbsolutePath() +
                    File.separator + "AggregateMultiplier.xml");
            if (cmDelegates != null && cmDelegates.stream().filter(Description::isActive).count() == 1) {
                Description cm = cmDelegates.stream().filter(Description::isActive).findFirst().get();
                aaeCmDesc = cm.getDescriptorAsAnalysisEngineDescription();
                cmFile = getDescriptorStoragePath(cm, descDir).toFile();
                cm.getDescriptor().setSourceUrl(cmFile.toURI().toURL());
                cm.getDescriptorAsAnalysisEngineDescription().toXML(FileUtilities.getWriterToFile(
                        cmFile), true);
                cm.setUimaDescPath(cmFile.getName());
                filesToDeleteOnSave.remove(cmFile.getName());
            } else if (cmDelegates != null && cmDelegates.stream().filter(Description::isActive).count() > 1) {
                aaeCmDesc.getMetaData().setName("JCoRe Multiplier AAE");
                aaeCmDesc.getMetaData().setDescription("This AAE descriptor directly contains the CAS multipliers added " +
                        "through the JCoRe pipeline builder. The AAE serves to bundle all the components together.");
                storeDescriptor(aaeCmDesc, cmFile);
            } else if (cmFile.exists()) {
                cmFile.delete();
            }

            File aaeFile = new File(descDir.getAbsolutePath() +
                    File.separator + "AggregateAnalysisEngine.xml");
            if (aaeDesc != null) {
                aaeDesc.getMetaData().setName("JCoRe Pipeline AAE");
                aaeDesc.getMetaData().setDescription("This AAE descriptor directly contains the analysis engines added " +
                        "through the JCoRe pipeline builder. The AAE serves to bundle all the components together.");
                storeDescriptor(aaeDesc, aaeFile);

                Stream<AnalysisEngineDescription> descStream = aeDelegates.stream().
                        // filter(desc -> !desc.getMetaDescription().isPear()).
                                filter(Description::isActive).
                        map(Description::getDescriptorAsAnalysisEngineDescription);
                final AnalysisEngineDescription integratedDelegatesAAE = createAAEWithIntegratedDelegates(descDirAll, "AggregateAnalysisEngineWithIntegratedDelegateDescriptors", aeDelegates, descStream, true, aeFlowController);
                storeDescriptor(integratedDelegatesAAE, new File(descDirAll.getAbsolutePath() + File.separator + AGGREGATE_ANALYSIS_ENGINE_WITH_INTEGRATED_DELEGATE_DESCRIPTORS_XML));
            } else if (aaeFile.exists()) {
                aaeFile.delete();
            }

            File ccFile = new File(descDir.getAbsolutePath() +
                    File.separator +
                    "AggregateConsumer.xml");
            if (ccFile.exists())
                ccFile.delete();
            if (ccDelegates != null && ccDelegates.stream().anyMatch(Description::isActive)) {
                final List<Description> activeCCs = ccDelegates.stream().filter(Description::isActive).collect(Collectors.toList());
                for (Description ccDesc : activeCCs) {
                    storeCCDescriptor(ccDesc, descDir);
                }
                if (activeCCs.size() == 1) {
                    final Description activeCCDescription = activeCCs.get(0);
                    ccDesc = (ResourceCreationSpecifier) activeCCDescription.getDescriptor();
                    ccFile = new File(descDir.getAbsolutePath() + File.separator + activeCCs.get(0).getUimaDescPath());
                } else if (activeCCs.size() > 1) {
                    ccDesc = createAAEWithImportedDelegates(descDir, "AggregateConsumer", ccDelegates, activeCCs.stream().map(Description::getDescriptorAsAnalysisEngineDescription), true, ccFlowController);
                    ccDesc.getMetaData().setName("JCoRe Consumer AAE");
                    ccDesc.getMetaData().setDescription("This consumer AAE descriptor directly contains the CAS consumers added " +
                            "through the JCoRe pipeline builder. The AAE serves to bundle all the components together.");

                    // And for the actual active pipeline descriptors directory
                    storeDescriptor(ccDesc, ccFile);
                }
            }

            // Storing a CPE descriptor
            storeCPE(descDir, cmFile, aaeFile, ccFile);
        } catch (SAXException | IOException | ResourceInitializationException e) {
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
        try (InputStream versionFileStream = getClass().getResourceAsStream("/version.txt")) {
            if (versionFileStream != null) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(new BufferedInputStream(versionFileStream)))) {
                    String version = br.lines().filter(Predicate.not(String::isBlank)).findAny().get();
                    try (BufferedWriter bw = new BufferedWriter(new FileWriter(directory.getAbsolutePath() + File.separator + "version-pipelinebuilder.txt", StandardCharsets.UTF_8))) {
                        bw.write(version);
                        bw.newLine();
                    }
                }
            }
        } catch (IOException e) {
            throw new PipelineIOException(e);
        }

        // Store the required Maven artifacts in the lib directory
        try {
            final File libDir = new File(directory.getAbsolutePath() + File.separator + DIR_LIB);
            if (populateLibDir) {
                if (libDir.exists()) {
                    log.debug("Removing all files from the library directory at {}", libDir);
                    Stream.of(libDir.listFiles()).forEach(File::delete);
                }
                log.debug("Storing all artifact files to {}", libDir);
                storeArtifacts(directory);
            }
        } catch (MavenException e) {
            throw new PipelineIOException(e);
        }

        // Store descriptions with their meta data
        try {
            serializeDescriptions(directory, SERIALIZED_CR_DESCS_FILE, crDescription);
            serializeDescriptions(directory, SERIALIZED_CM_DESCS_FILE, cmDelegates);
            serializeDescriptions(directory, SERIALIZED_AE_FLOW_CONTROLLER_DESCS_FILE, aeFlowController);
            serializeDescriptions(directory, SERIALIZED_AE_DESCS_FILE, aeDelegates);
            serializeDescriptions(directory, SERIALIZED_CC_FLOW_CONTROLLER_DESCS_FILE, ccFlowController);
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

        // Store the parent POM, if set
        if (parentPom != null) {
            ObjectMapper om = new ObjectMapper();
            try {
                om.writeValue(Path.of(directory.getAbsolutePath(), "pipelineParentPomSource.json").toFile(), parentPom);
            } catch (IOException e) {
                throw new PipelineIOException(e);
            }
        }
    }

    /**
     * <p>Creates a Collection Processing Engine (CPE) descriptor for all components in the pipeline and stores it.</p>
     *
     * @param descDir The directory to store the descriptors to.
     * @param cmFile  The CAS multiplier descriptor descriptor file.
     * @param aaeFile The analysis engine aggregate descriptor file.
     * @param ccFile  The CAS consumer aggregate descriptor file.
     */
    private void storeCPE(File descDir, File cmFile, File aaeFile, File ccFile) {
        try {
            final File cpeAAEFile = new File(descDir.getAbsolutePath() + File.separator + CPE_AAE_DESC_NAME);
            final File cpeFile = new File(descDir.getAbsolutePath() + File.separator +
                    "CPE.xml");
            if (ccDelegates == null || ccDelegates.stream().map(Description::getDescriptor).noneMatch(CasConsumer.class::isInstance)) {

                final CPE cpe = new CPE();
                if (crDescription != null) {
                    cpe.setCollectionReader(crDescription);
                }
                AnalysisEngineDescription cpeAAE = AnalysisEngineFactory.createEngineDescription();

                boolean multipleDeploymentAllowed = true;
                cpeAAE.getDelegateAnalysisEngineSpecifiersWithImports().clear();
                if (cmDelegates.stream().anyMatch(Description::isActive)) {
                    Import_impl cmImport = new Import_impl();
                    cmImport.setLocation(cmFile.getName());
                    cmImport.setSourceUrl(cmFile.toURI().toURL());
                    cpeAAE.getDelegateAnalysisEngineSpecifiersWithImports().put(aaeCmDesc.getMetaData().getName(), cmImport);
                    final boolean cmMultipleDeploymentAllowed = aaeCmDesc.getAnalysisEngineMetaData().getOperationalProperties().isMultipleDeploymentAllowed();
                    if (!cmMultipleDeploymentAllowed)
                        log.warn("The CAS multiplier used for the CPE Aggregate Analysis engine does not support multiple deployments. Thus, the whole CPE will basically run singlethreaded.");
                    multipleDeploymentAllowed &= cmMultipleDeploymentAllowed;
                }
                if (aaeDesc != null) {
                    Import_impl aaeImport = new Import_impl();
                    aaeImport.setLocation(aaeFile.getName());
                    aaeImport.setSourceUrl(aaeFile.toURI().toURL());
                    cpeAAE.getDelegateAnalysisEngineSpecifiersWithImports().put(aaeDesc.getMetaData().getName(), aaeImport);
                    final boolean aaeMultipleDeploymentAllowed = aaeDesc.getAnalysisEngineMetaData().getOperationalProperties().isMultipleDeploymentAllowed();
                    if (!aaeMultipleDeploymentAllowed)
                        log.warn("The aggregate collecting the actual analysis engines used for the CPE Aggregate Analysis engine does not support multiple deployments. Thus, the whole CPE will basically run singlethreaded.");
                    multipleDeploymentAllowed &= aaeMultipleDeploymentAllowed;

                }
                if (ccDelegates != null && ccDelegates.stream().anyMatch(Description::isActive)) {
                    Import_impl ccImport = new Import_impl();
                    ccImport.setLocation(ccFile.getName());
                    ccImport.setSourceUrl(ccFile.toURI().toURL());
                    cpeAAE.getDelegateAnalysisEngineSpecifiersWithImports().put(ccDesc.getMetaData().getName(), ccImport);
                    boolean ccMultipleDeploymentAllowed;
                    if (ccDesc instanceof AnalysisEngineDescription)
                        ccMultipleDeploymentAllowed = ((AnalysisEngineDescription) ccDesc).getAnalysisEngineMetaData().getOperationalProperties().isMultipleDeploymentAllowed();
                    else
                        ccMultipleDeploymentAllowed = ((CasConsumerDescription) ccDesc).getCasConsumerMetaData().getOperationalProperties().isMultipleDeploymentAllowed();
                    if (!ccMultipleDeploymentAllowed)
                        log.warn("The consumer (potentially an aggregate) used for the CPE Aggregate Analysis engine does not support multiple deployments. Thus, the whole CPE will basically run singlethreaded.");
                    multipleDeploymentAllowed &= ccMultipleDeploymentAllowed;
                }
                if (!multipleDeploymentAllowed)
                    log.warn("The sole AggregateAnalysisEngine created for the CPE cannot allow multiple deployment because one of its delegate does not. This will render multithreading ineffective.");
                cpeAAE.getAnalysisEngineMetaData().getOperationalProperties().setMultipleDeploymentAllowed(multipleDeploymentAllowed);
                Stream<ResourceCreationSpecifier> descriptorsForFlow = Stream.of(this.aaeCmDesc, aaeDesc);
                if (ccDelegates != null && ccDelegates.stream().anyMatch(Description::isActive))
                    descriptorsForFlow = Stream.concat(descriptorsForFlow, Stream.of(ccDesc));
                String[] flow = descriptorsForFlow.filter(Objects::nonNull).map(ResourceCreationSpecifier::getMetaData).map(ResourceMetaData::getName).toArray(String[]::new);
                ((FixedFlow) cpeAAE.getAnalysisEngineMetaData().getFlowConstraints()).setFixedFlow(flow);
                cpeAAE.toXML(
                        FileUtilities.getWriterToFile(
                                cpeAAEFile));
                cpe.setAnalysisEngine(cpeAAEFile.getName(), "CPE AAE");

                for (CpeCasProcessor casProcessor : cpe.getDescription().getCpeCasProcessors().getAllCpeCasProcessors()) {
                    // this corresponds to the following XML element in the CAS.xml:
                    // <errorRateThreshold action="terminate" value="0/1"/>
                    // In "value", the first number is the maximum number of errors and the second number is
                    // the window (a number of documents) in which the maximum number of errors are allowed to happen.
                    // We here want to express that no error is allowed by default. The CPE should just abort and
                    // exit on errors.
                    casProcessor.setMaxErrorCount(0);
                    casProcessor.setMaxErrorSampleSize(1);
                }

                final CpeDescription cpeDescription = cpe.getDescription();
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
        } catch (Exception e) {
            log.error("Could not store the CPE descriptor: ", e);
        }
    }

    // I'm actually not sure why we need 'allDelegates' and 'aaeElements'. Perhaps we don't. Try it when there is time.
    private AnalysisEngineDescription createAAEWithImportedDelegates(File descDir, String name, List<Description> allDelegates, Stream<AnalysisEngineDescription> aaeElements, boolean filterDeactivated, Description flowControllerDescription) throws ResourceInitializationException, SAXException, IOException, InvalidXMLException, PipelineIOException {
        AnalysisEngineDescription aaeDesc = flowControllerDescription == null || !flowControllerDescription.isActive() ? AnalysisEngineFactory.createEngineDescription(aaeElements.toArray(AnalysisEngineDescription[]::new)) : AnalysisEngineFactory.createEngineDescription(flowControllerDescription.getDescriptorAsFlowControllerDescriptor(), aaeElements.toArray(AnalysisEngineDescription[]::new));
        Map<String, MetaDataObject> delegatesWithImports = aaeDesc.getDelegateAnalysisEngineSpecifiersWithImports();
        delegatesWithImports.clear();
        List<String> flowNames = new ArrayList<>();
        boolean multipleDeploymentAllowed = true;
        for (Description description : allDelegates) {
            final boolean currentComponentAllowsMultipleDeployment = description.getDescriptorAsAnalysisEngineDescription().getAnalysisEngineMetaData().getOperationalProperties().isMultipleDeploymentAllowed();
            if (!currentComponentAllowsMultipleDeployment) {
                log.warn("The component {} does not allow multiple deployment. Thus, multiple deployment won't be allowed for the whole AAE with name {}.", description.getName(), name);
            }
            multipleDeploymentAllowed &= currentComponentAllowsMultipleDeployment;
            if (description.isActive() || !filterDeactivated) {
                Import imp = new Import_impl();
                imp.setLocation(description.getName() + ".xml");
                delegatesWithImports.put(description.getName(), imp);
                final File destination = getDescriptorStoragePath(description, descDir).toFile();
                description.getDescriptorAsAnalysisEngineDescription().toXML(FileUtilities.getWriterToFile(destination), true);
                flowNames.add(description.getName());
            }
        }
        // necessary for relative resolution of imports
        aaeDesc.setSourceUrl(descDir.toURI().toURL());
        try {
            // This is required when using PEARs. This call causes the internal call to resolveDelegateAnalysisEngineImports()
            // which completes PEAR imports.
            // TODO then only to it when there a PEARs in the AAE
            final Optional<Description> pearOpt = allDelegates.stream().filter(d -> d.getMetaDescription().isPear()).findAny();
            if (pearOpt.isPresent())
                aaeDesc.getDelegateAnalysisEngineSpecifiers();
        } catch (InvalidXMLException e) {
            log.debug("An InvalidXMLException was thrown. This could be due to actually invalid XML but also because a type descriptor import couldn't be found. Loading dependencies.");
            addLibrariesToClassPath();
            aaeDesc.getDelegateAnalysisEngineSpecifiers();
        }
        ((FixedFlow) aaeDesc.getAnalysisEngineMetaData().getFlowConstraints()).setFixedFlow(flowNames.toArray(new String[0]));
        aaeDesc.getAnalysisEngineMetaData().setName(name);
        if (!multipleDeploymentAllowed) {
            log.warn("Deactivating multiple deployments for the AAE {} because at least one delegate does not support multiple deployments.", name);
            aaeDesc.getAnalysisEngineMetaData().getOperationalProperties().setMultipleDeploymentAllowed(false);
        }
        return aaeDesc;
    }

    private AnalysisEngineDescription createAAEWithIntegratedDelegates(File descDir, String name, List<Description> allDelegates, Stream<AnalysisEngineDescription> aaeElements, boolean filterDeactivated, Description flowControllerDescription) throws ResourceInitializationException, SAXException, IOException, InvalidXMLException, PipelineIOException {
        AnalysisEngineDescription aaeDesc = flowControllerDescription == null || !flowControllerDescription.isActive() ? AnalysisEngineFactory.createEngineDescription(aaeElements.toArray(AnalysisEngineDescription[]::new)) : AnalysisEngineFactory.createEngineDescription(flowControllerDescription.getDescriptorAsFlowControllerDescriptor(), aaeElements.toArray(AnalysisEngineDescription[]::new));
//        List<String> flowNames = new ArrayList<>();
        boolean multipleDeploymentAllowed = true;
        for (Description description : allDelegates) {
            final boolean currentComponentAllowsMultipleDeployment = description.getDescriptorAsAnalysisEngineDescription().getAnalysisEngineMetaData().getOperationalProperties().isMultipleDeploymentAllowed();
            if (!currentComponentAllowsMultipleDeployment) {
                log.warn("The component {} does not allow multiple deployment. Thus, multiple deployment won't be allowed for the whole AAE with name {}.", description.getName(), name);
            }
            multipleDeploymentAllowed &= currentComponentAllowsMultipleDeployment;
//            if (description.isActive() || !filterDeactivated) {
//                flowNames.add(description.getName());
//            }
        }
        // necessary for relative resolution of imports
        aaeDesc.setSourceUrl(descDir.toURI().toURL());
        try {
            // This is required when using PEARs. This call causes the internal call to resolveDelegateAnalysisEngineImports()
            // which completes PEAR imports.
            // TODO then only to it when there a PEARs in the AAE
            final Optional<Description> pearOpt = allDelegates.stream().filter(d -> d.getMetaDescription().isPear()).findAny();
            if (pearOpt.isPresent())
                aaeDesc.getDelegateAnalysisEngineSpecifiers();
        } catch (InvalidXMLException e) {
            log.debug("An InvalidXMLException was thrown. This could be due to actually invalid XML but also because a type descriptor import couldn't be found. Loading dependencies.");
            addLibrariesToClassPath();
            aaeDesc.getDelegateAnalysisEngineSpecifiers();
        }
//        ((FixedFlow) aaeDesc.getAnalysisEngineMetaData().getFlowConstraints()).setFixedFlow(flowNames.toArray(new String[0]));
        aaeDesc.getAnalysisEngineMetaData().setName(name);
        if (!multipleDeploymentAllowed) {
            log.warn("Deactivating multiple deployments for the AAE {} because at least one delegate does not support multiple deployments.", name);
            aaeDesc.getAnalysisEngineMetaData().getOperationalProperties().setMultipleDeploymentAllowed(false);
        }
        return aaeDesc;
    }

    private void storeAllDescriptors(File descDirAll) throws IOException, SAXException, InvalidXMLException, ResourceInitializationException, PipelineIOException {
        if (!descDirAll.exists()) {
            descDirAll.mkdirs();
        }
        // Clear the desc all directory to avoid leftover descriptors which can cause issues when loading
        Stream.of(descDirAll.listFiles()).forEach(File::delete);
        // store all descriptors in the desc_all directory
        if (crDescription != null)
            storeDescriptor(crDescription.getDescriptorAsCollectionReaderDescription(), getDescriptorStoragePath(crDescription, descDirAll).toFile());
        for (Description cmDelegate : cmDelegates)
            storeDescriptor(cmDelegate.getDescriptorAsAnalysisEngineDescription(), getDescriptorStoragePath(cmDelegate, descDirAll).toFile());
        if (aeFlowController != null)
            storeDescriptor(aeFlowController.getDescriptorAsFlowControllerDescriptor(), getDescriptorStoragePath(aeFlowController, descDirAll).toFile());
        for (Description aeDelegate : aeDelegates)
            storeDescriptor(aeDelegate.getDescriptorAsAnalysisEngineDescription(), getDescriptorStoragePath(aeDelegate, descDirAll).toFile());
        if (ccFlowController != null)
            storeDescriptor(ccFlowController.getDescriptorAsFlowControllerDescriptor(), getDescriptorStoragePath(ccFlowController, descDirAll).toFile());
        for (Description ccDelegate : ccDelegates)
            storeCCDescriptor(ccDelegate, descDirAll);

        // Store AAEs
        if (!cmDelegates.isEmpty() && cmDelegates.size() > 1) {
            final AnalysisEngineDescription aae = createAAEWithImportedDelegates(descDirAll, "AggregateMultiplier", cmDelegates, cmDelegates.stream().map(Description::getDescriptorAsAnalysisEngineDescription), false, null);
            storeDescriptor(aae, Paths.get(descDirAll.getAbsolutePath(), aae.getMetaData().getName() + ".xml").toFile());
        }
        if (!aeDelegates.isEmpty() && aeDelegates.size() > 1) {
            final AnalysisEngineDescription aae = createAAEWithImportedDelegates(descDirAll, "AggregateAnalysisEngine", aeDelegates, aeDelegates.stream().map(Description::getDescriptorAsAnalysisEngineDescription), false, aeFlowController);
            storeDescriptor(aae, Paths.get(descDirAll.getAbsolutePath(), aae.getMetaData().getName() + ".xml").toFile());
        }
        if (!ccDelegates.isEmpty() && ccDelegates.size() > 1) {
            final AnalysisEngineDescription aae = createAAEWithImportedDelegates(descDirAll, "AggregateConsumer", ccDelegates, ccDelegates.stream().map(Description::getDescriptorAsAnalysisEngineDescription), false, ccFlowController);
            storeDescriptor(aae, Paths.get(descDirAll.getAbsolutePath(), aae.getMetaData().getName() + ".xml").toFile());
        }

    }

    private void storeDescriptor(ResourceCreationSpecifier spec, File path) throws IOException, SAXException {
        spec.setSourceUrl(path.toURI().toURL());
        try (final BufferedWriter writer = FileUtilities.getWriterToFile(path)) {
            if (spec instanceof AnalysisEngineDescription) {
                ((AnalysisEngineDescription) spec).toXML(writer, true);
            } else {
                spec.toXML(writer);
            }
        }
    }

    private Path getDescriptorStoragePath(Description desc, File destinationDir) {
        final String filename = desc.getName() + ".xml";
        return Paths.get(destinationDir.getAbsolutePath(), filename);
    }

    private void storeCCDescriptor(Description ccDesc, File descDir) throws SAXException, IOException {
        File ccFile;
        if (!ccDesc.getName().toLowerCase().contains("writer") && !ccDesc.getName().toLowerCase().contains("consumer"))
            throw new IllegalStateException("The CAS consumer descriptor " +
                    ccDesc.getName() + " at " +
                    ccDesc.getDescriptor().getSourceUrlString() + " does not specify 'writer' or 'consumer' " +
                    "in its name. By convention, consumers must do this to be recognized as consumer.");
        ccFile = getDescriptorStoragePath(ccDesc, descDir).toFile();
        ccDesc.setUri(ccFile.toURI());
        ccDesc.setUimaDescPath(ccFile.getName());
        if (ccDesc.getDescriptor() instanceof AnalysisEngineDescription)
            ccDesc.getDescriptorAsAnalysisEngineDescription().toXML(FileUtilities.getWriterToFile(ccFile), true);
        else
            ccDesc.getDescriptor().toXML(FileUtilities.getWriterToFile(ccFile));
        filesToDeleteOnSave.remove(ccFile.getName());
    }

    /**
     * Stores the Maven artifacts in the lib/ directory directly beneath the given pipeline <tt>directory</tt>.
     *
     * @param directory
     * @return
     */
    public void storeArtifacts(File directory) throws MavenException {
        File libDir = new File(directory.getAbsolutePath() + File.separator + DIR_LIB);
        if (!libDir.exists())
            libDir.mkdirs();
        Stream<Description> descriptions = Stream.empty();
        if (crDescription != null && crDescription.getMetaDescription() != null)
            descriptions = Stream.concat(descriptions, Stream.of(crDescription));
        if (cmDelegates != null)
            descriptions = Stream.concat(descriptions, cmDelegates.stream().filter(d -> Objects.nonNull(d.getMetaDescription())));
        if (aeFlowController != null)
            descriptions = Stream.concat(descriptions, Stream.of(aeFlowController));
        if (aeDelegates != null)
            descriptions = Stream.concat(descriptions, aeDelegates.stream().filter(d -> !d.getMetaDescription().isPear() && Objects.nonNull(d.getMetaDescription())));
        if (ccFlowController != null)
            descriptions = Stream.concat(descriptions, Stream.of(ccFlowController));
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
        AetherUtilities.storeArtifactsWithDependencies(parentPom, descriptions.map(d -> d.getMetaDescription().getMavenArtifact()), libDir);
    }

    private void serializeDescriptions(File pipelineStorageDir, String targetFileName, Object descriptions) throws IOException {
        if (!pipelineStorageDir.exists())
            pipelineStorageDir.mkdirs();
        File targetFile = new File(pipelineStorageDir.getAbsolutePath() + File.separatorChar + targetFileName);
        final ObjectMapper om = new ObjectMapper();
        om.addMixIn(MetaDescription.class, MetaDescriptionPipelineStorageMixin.class);
        om.writeValue(FileUtilities.getWriterToFile(targetFile), descriptions);
    }

    private <T> T deserializeDescriptions(File pipelineStorageDir, String sourceFileName, TypeReference<?> typeReference) throws IOException, ClassNotFoundException {
        File sourceFile = new File(pipelineStorageDir.getAbsolutePath() + File.separatorChar + sourceFileName);
        if (!sourceFile.exists())
            return null;
        final ObjectMapper om = new ObjectMapper();
        try (BufferedReader reader = FileUtilities.getReaderFromFile(sourceFile)) {
            return (T) om.readValue(reader, typeReference);
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
                    crDescription = deserializeDescriptions(loadDirectory, SERIALIZED_CR_DESCS_FILE, new TypeReference<Description>() {
                    });
                    cmDelegates = deserializeDescriptions(loadDirectory, SERIALIZED_CM_DESCS_FILE, new TypeReference<List<Description>>() {
                    });
                    aeFlowController = deserializeDescriptions(loadDirectory, SERIALIZED_AE_FLOW_CONTROLLER_DESCS_FILE, new TypeReference<Description>() {
                    });
                    aeDelegates = deserializeDescriptions(loadDirectory, SERIALIZED_AE_DESCS_FILE, new TypeReference<List<Description>>() {
                    });
                    ccFlowController = deserializeDescriptions(loadDirectory, SERIALIZED_CC_FLOW_CONTROLLER_DESCS_FILE, new TypeReference<Description>() {
                    });
                    ccDelegates = deserializeDescriptions(loadDirectory, SERIALIZED_CC_DESCS_FILE, new TypeReference<List<Description>>() {
                    });
                }
            } catch (ClassNotFoundException e) {
                throw new PipelineIOException(e);
            }

            // Load the parent pipeline POM
            final ObjectMapper om = new ObjectMapper();
            final File parentPomFile = Path.of(loadDirectory.getAbsolutePath(), ("pipelineParentPomSource.json")).toFile();
            if (parentPomFile.exists())
                parentPom = om.readValue(parentPomFile, MavenArtifact.class);

            File descDir = new File(loadDirectory.getAbsolutePath() + File.separator + DIR_DESC_ALL);
            if (!descDir.exists()) {
                File allDescDir = descDir;
                descDir = new File(loadDirectory.getAbsolutePath() + File.separator + DIR_DESC);
                log.debug("Directory {} does not exist, falling back to {}", allDescDir, descDir);
            }
            if (!descDir.exists())
                throw new PipelineIOException("The JCoReUIMAPipeline directory "
                        + loadDirectory + " does not have the descriptor sub directory " + DIR_DESC_ALL + " or " + DIR_DESC);
            File[] xmlFiles = descDir.listFiles(f -> f.getName().endsWith(".xml"));
            List<CollectionReaderDescription> crDescs = new ArrayList<>();
            List<AnalysisEngineDescription> cmDescs = new ArrayList<>();
            List<AnalysisEngineDescription> aaeCmDescs = new ArrayList<>();
            List<AnalysisEngineDescription> aeDescs = new ArrayList<>();
            List<AnalysisEngineDescription> aaeDescs = new ArrayList<>();
            List<ResourceCreationSpecifier> ccDescs = new ArrayList<>();
            List<ResourceCreationSpecifier> aaeCcDescs = new ArrayList<>();
            List<ResourceCreationSpecifier> flowContrDescs = new ArrayList<>();
            for (File xmlFile : xmlFiles) {
                // don't load the CPE AAE descriptor, it is solely needed when using the CPE descriptor on its own
                if (xmlFile.getName().equals(CPE_AAE_DESC_NAME))
                    continue;
                // don't load the AAE with integrated delegate descriptors, this is just a helper
                if (xmlFile.getName().equals(AGGREGATE_ANALYSIS_ENGINE_WITH_INTEGRATED_DELEGATE_DESCRIPTORS_XML))
                    continue;
                XMLParser parser = UIMAFramework.getXMLParser();
                ResourceCreationSpecifier spec = null;
                try {
                    spec = (ResourceCreationSpecifier) parser.parseResourceSpecifier(
                            new XMLInputSource(xmlFile));
                } catch (InvalidXMLException e) {
                    if (log.isDebugEnabled()) {
                        List<String> messages = new ArrayList<>();
                        Throwable cause = e;
                        do {
                            messages.add(cause.getMessage());
                            cause = cause.getCause();
                        } while (cause != null);
                        log.debug("File {} could not be parsed as a UIMA component and is skipped: {}", xmlFile, String.join("; ", messages));
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
                    } else if (spec instanceof FlowControllerDescription) {
                        flowContrDescs.add(spec);
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


            // the descriptions are loaded at the beginning of the method, if existent
            long time = System.currentTimeMillis();
            time = System.currentTimeMillis() - time;
            log.debug("Loading of dependencies took {}ms ({}s)", time, time / 1000);
            if (crDescription == null && crDescs != null && !crDescs.isEmpty())
                crDescription = new Description(crDescs.get(0).getSourceUrl());
            if (!cmDescs.isEmpty())
                aaeCmDesc = cmDescs.get(0);
            if (aaeCmDescs.size() == 1)
                aaeCmDesc = aaeCmDescs.get(0);
            if (aaeCmDescs.size() > 1)
                setAaeDesc(descDir, cmDescs, aaeCmDescs, "CAS multiplier", desc -> aaeCmDesc = desc);
            if (!flowContrDescs.isEmpty() && aeFlowController != null)
                setDescriptorByName(aeFlowController, flowContrDescs);
            if (!aeDescs.isEmpty())
                aaeDesc = aeDescs.get(0);
            if (aaeDescs.size() == 1)
                aaeDesc = aaeDescs.get(0);
            if (aeDescs.size() > 1 && aaeDescs.isEmpty()) {
                log.warn("Found analysis engine descriptions but no AAE grouping them together. Creating a new aggregate for grouping. A reorder might be necessary.");
                aaeDesc = AnalysisEngineFactory.createEngineDescription(aeDescs, aeDescs.stream().map(ae -> ae.getAnalysisEngineMetaData().getName()).collect(toList()), null, null, null);
            }
            if (aaeDescs.size() > 1)
                setAaeDesc(descDir, aeDescs, aaeDescs, "analysis engine", desc -> aaeDesc = desc);
            if (!flowContrDescs.isEmpty() && ccFlowController != null)
                setDescriptorByName(ccFlowController, flowContrDescs);
            if (!ccDescs.isEmpty())
                ccDesc = ccDescs.get(0);
            if (aaeCcDescs.size() == 1)
                ccDesc = aaeCcDescs.get(0);
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
            try {
                if (crDescription != null && !crDescs.isEmpty())
                    crDescription.setDescriptor(crDescs.get(0));
                if (aaeCmDesc != null && !cmDelegates.isEmpty())
                    setAaeDescriptors(aaeCmDesc, cmDelegates, cmDescs, "CAS Multiplier");
                if (aaeDesc != null && !aeDelegates.isEmpty())
                    setAaeDescriptors(aaeDesc, aeDelegates, aeDescs, "Analysis Engine");
                if (ccDesc != null && !ccDelegates.isEmpty() && ccDesc instanceof AnalysisEngineDescription)
                    setAaeDescriptors((AnalysisEngineDescription) ccDesc, ccDelegates, ccDescs, "CAS Consumer");
            } catch (Exception e) {
                log.warn("Could not set descriptor files from the {}/ directory to the serialized meta descriptions. Changes in the descriptors that have not been stored in the meta descriptions won't be available.", DIR_DESC_ALL, e);
            }
        } catch (IOException | InvalidXMLException | URISyntaxException | ResourceInitializationException e) {
            throw new PipelineIOException(e);
        }
        return this;
    }

    /**
     * <p>Sets the UIMA descriptor to the given description by matching the names between descriptor and description.</p>
     *
     * @param description The description to set the descriptor to.
     * @param descriptors The list of loaded flow controller descriptors to select the matching descriptor from.
     */
    private void setDescriptorByName(Description description, List<ResourceCreationSpecifier> descriptors) {
        String name = description.getName();
        for (ResourceCreationSpecifier spec : descriptors) {
            String descriptorName = spec.getMetaData().getName();
            if (name.equals(descriptorName)) {
                description.setDescriptor(spec);
                return;
            }
        }
        throw new IllegalStateException("Tried to find the UIMA descriptor for the description with name '" + name + "' but could not find one. Available descriptor names were: " + descriptors.stream().map(d -> d.getMetaData().getName()).collect(joining(", ")));
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
    private void setAaeDescriptors(AnalysisEngineDescription aae, List<Description> descriptions, List<? extends ResourceCreationSpecifier> loadedDescriptors, String type) throws PipelineIOException, InvalidXMLException {
        if (!aae.isPrimitive()) {
            Map<String, Description> descByName = descriptions.stream().collect(Collectors.toMap(Description::getName, Function.identity()));
            Map<String, ResourceSpecifier> specByName = loadedDescriptors.stream().collect(Collectors.toMap(s -> s.getMetaData().getName(), Function.identity()));
            FlowConstraints flowConstraints = aae.getAnalysisEngineMetaData().getFlowConstraints();
            if (!(flowConstraints instanceof FixedFlow))
                throw new PipelineIOException(String.format("The %s aggregate does not define a FixedFlow. Only FixedFlow constraints are currently supported.", type));
            FixedFlow flow = (FixedFlow) flowConstraints;
            for (int i = 0; i < flow.getFixedFlow().length; ++i) {
                String component = flow.getFixedFlow()[i];
                ResourceSpecifier descriptor;
                descriptor = specByName.get(component);
                if (descriptor == null) {
                    // we need to check if this is an AAE with integrated descriptors
                    addLibrariesToClassPath();
                    final MetaDataObject metaDataObject = aae.getDelegateAnalysisEngineSpecifiersWithImports().get(component);
                    if (metaDataObject == null)
                        throw new IllegalStateException("The AAE '" + aae.getMetaData().getName() + "' specifies the flow component '" + component + "' but does not list it as a delegate.");
                    // the meta data object can be two things: an import - in which case we have an error at our hands - or the descriptor itself when it was integrated into the AAE.
                    if (!(metaDataObject instanceof AnalysisEngineDescription))
                        throw new IllegalStateException("The " + type + " AAE specifies the component key " + component + " but no descriptor loaded from the descAll/ directory has this name. Names in the stored descriptors may not be changed outside of the Pipeline Builder or they can possibly not be loaded any more.");
                } else if (i < descriptions.size()) {
                    if (!descByName.containsKey(component))
                        throw new IllegalStateException("The " + type + " AAE specifies the component key " + component + " but no descriptor has this name. The descriptor names and the AAE keys must match.");
                    descByName.get(component).setDescriptor(descriptor);
                }
            }
            log.debug("For the {} aggregate, the following delegate descriptors were set: {}", type, String.join(", ", flow.getFixedFlow()));
            // We might have an AAE which has been imported as a component and, thus, has a description. Then, we need to set the descriptor to the description.
            // This is not necessary when the AAE is just the standard pipeline AAE created automatically on pipeline storage. That AAE has no description anyway.
            descriptions.stream().filter(d -> d.getName().equals(aae.getMetaData().getName())).findAny().ifPresent(d -> d.setDescriptor(aae));
        } else {
            if (descriptions.size() > 1)
                log.error("The {} is not an aggregate but there are {} descriptions with the following names: {}", type, descriptions.size(), descriptions.stream().map(Description::getName).collect(joining(", ")));
            descriptions.get(0).setDescriptor(aae);
        }
    }

    private void addLibrariesToClassPath() throws PipelineIOException {
        if (!areLibrariesLoaded) {
            log.info("Loading pipeline libraries. This is required to resolve AAE descriptor imports.");
            getClasspathElements().forEach(JarLoader::addJarToClassPath);
        }
        areLibrariesLoaded = true;
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
        // This function delivers canonical strings for URIs that denote files. Which is what we need here since
        // all descriptors are stored as files in the desc/ an descAll/ directories.
        // Calling URL::toString caused issues in the past where the file URI did not equal the URI generated
        // by UIMA for the same descriptor file.
        Function<URL, String> url2String = url -> {
            try {
                return new File(url.toString().replaceAll(" ", "%20")).getCanonicalPath();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        };
        Set<String> aeUris = aeDescs.stream().map(AnalysisEngineDescription::getSourceUrl).map(url2String).collect(toSet());
        Set<String> aaeUris = aaeDescs.stream().map(AnalysisEngineDescription::getSourceUrl).map(url2String).collect(toSet());
        Set<String> delegateUris;
        try {
            delegateUris = new HashSet<>();
            for (AnalysisEngineDescription aaeDesc : aaeDescs) {
                delegateUris.addAll(aaeDesc.getDelegateAnalysisEngineSpecifiers().values().stream().map(ResourceSpecifier::getSourceUrl).map(url2String).collect(toList()));
            }
        } catch (InvalidXMLException e) {
            log.debug("An InvalidXMLException was thrown while loading descriptors from file. This could be due to actually invalid XML but also because a type descriptor import couldn't be found. Loading dependencies.");
            addLibrariesToClassPath();
            delegateUris = new HashSet<>();
            for (AnalysisEngineDescription aaeDesc : aaeDescs) {
                delegateUris.addAll(aaeDesc.getDelegateAnalysisEngineSpecifiers().values().stream().map(ResourceSpecifier::getSourceUrl).map(url2String).collect(toList()));
            }
        }
        Sets.SetView<String> topAAEs = Sets.difference(aaeUris, delegateUris);
        Sets.SetView<String> aesNotInAAE = Sets.difference(aeUris, delegateUris);
        final Sets.SetView<String> delegateAAEs = Sets.difference(aaeUris, topAAEs);
        // add the AAEs that are delegates of the top AAE to the AEs. This is required later to assemble the AAE tree structure in setAaeDescriptors
        aaeDescs.stream().filter(aae -> delegateAAEs.contains(url2String.apply(aae.getSourceUrl()))).forEach(aeDescs::add);

        if (topAAEs.size() > 1)
            throw new PipelineIOException("There are multiple " + componentType + "s in " + descDir.getAbsolutePath()
                    + " that don't have a common super AAE. The pipeline cannot be built because it is unknown which to use.");

        if (!topAAEs.isEmpty()) {
            String topAAEUri = topAAEs.iterator().next();
            descSetter.accept(aaeDescs.stream().filter(aaeDesc -> url2String.apply(aaeDesc.getSourceUrl()).equals(topAAEUri)).findAny().get());
            if (!aesNotInAAE.isEmpty())
                log.warn("The AAE {} is used for the " + componentType + " part of the pipeline. Primitive Analysis Engines " +
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
            avoidArtifactVersionConflicts(aeDesc);
        } else {
            // For PEARs, we create an extra AAE that only wraps the PEAR. The idea is that we do not need
            // special treatment for PEARs then in a lot of places
            final AnalysisEngineDescription_impl aaePear = new AnalysisEngineDescription_impl();
            final Map<String, MetaDataObject> delegatesWithImports = aaePear.getDelegateAnalysisEngineSpecifiersWithImports();
            Import_impl imp = new Import_impl();
            File pearDescriptorFile = new File(aeDesc.getLocation());
            imp.setLocation(pearDescriptorFile.toURI().toString());
            delegatesWithImports.put(aeDesc.getName(), imp);
            List<String> flowNames = Collections.singletonList(aeDesc.getName());

            aaePear.getAnalysisEngineMetaData().setFlowConstraints(new FixedFlow_impl());
            ((FixedFlow) aaePear.getAnalysisEngineMetaData().getFlowConstraints()).setFixedFlow(flowNames.toArray(new String[0]));
            try {
                aaePear.getDelegateAnalysisEngineSpecifiers();
                aeDesc = aeDesc.clone();
                aeDesc.setDescriptor(aaePear);
            } catch (CloneNotSupportedException | InvalidXMLException e) {
                e.printStackTrace();
            }
        }

        avoidNamingCollisions(aeDesc);
        aeDelegates.add(aeDesc);
    }

    private void avoidNamingCollisions(Description desc) {
        final Multiset<String> existingDescriptorNames = getExistingDescriptorNames();
        int i = 1;
        String basename = desc.getName();
        while (existingDescriptorNames.contains(desc.getName())) {
            desc.setName(basename + " " + i++);
            ((ResourceCreationSpecifier) desc.getDescriptor()).getMetaData().setName(desc.getName());
        }
    }

    /**
     * Returns the name of all component descriptions (not descriptors) added to this pipeline in a multiset. Thus,
     * duplicates will also be returned here for validity checks.
     *
     * @return A multiset of component description names.
     */
    public Multiset<String> getExistingDescriptorNames() {
        Multiset<String> names = HashMultiset.create();
        if (crDescription != null) names.add(crDescription.getName());
        cmDelegates.stream().map(Description::getName).forEach(names::add);
        if (aeFlowController != null) names.add(aeFlowController.getName());
        aeDelegates.stream().map(Description::getName).forEach(names::add);
        if (ccFlowController != null) names.add(ccFlowController.getName());
        ccDelegates.stream().map(Description::getName).forEach(names::add);
        return names;
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
        avoidArtifactVersionConflicts(multiplier);
        avoidNamingCollisions(multiplier);
        cmDelegates.add(multiplier);
    }

    /**
     * Retrieves a Maven artifact of already added components that equals the Maven artifact of the passed
     * description except its version. If such an artifact is found, its version is set to the added description version.
     * The goal is to synchonize the versions to avoid version conflicts.
     *
     * @param description A description to be added to the pipeline that should by synched to existing Maven artifact versions, if any.
     */
    private void avoidArtifactVersionConflicts(Description description) {
        if (description.getMetaDescription() != null && description.getMetaDescription().getMavenArtifact() != null) {
            final MavenArtifact artifact = description.getMetaDescription().getMavenArtifact();
            final Optional<MavenArtifact> anyExistingArtifactForComponent = getMavenComponentArtifacts().filter(a -> a.getArtifactId().equalsIgnoreCase(artifact.getArtifactId())
                    && a.getGroupId().equalsIgnoreCase(artifact.getGroupId())
                    && ((a.getClassifier() == null && artifact.getClassifier() == null) || a.getClassifier().equalsIgnoreCase(artifact.getClassifier()))
                    && a.getPackaging().equalsIgnoreCase(artifact.getPackaging())).findAny();
            anyExistingArtifactForComponent.ifPresent(mavenArtifact -> description.getMetaDescription().getMavenArtifact().setVersion(mavenArtifact.getVersion()));
        }
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
            final Stream<MavenArtifact> mavenArtifactStream = getMavenComponentArtifacts();
            return mavenArtifactStream.flatMap(artifact -> {
                try {
                    return AetherUtilities.getDependencies(artifact);
                } catch (MavenException e) {
                    log.error("Maven exception while trying to get transitive dependencies of artifact {}:{}:{}",
                            artifact.getGroupId(), artifact.getArtifactId(), artifact.getVersion(), e);
                }
                return null;
            }).map(Artifact::getFile);
        }
    }

    public Stream<MavenArtifact> getMavenComponentArtifacts() {
        List<Stream<MavenArtifact>> artifactList = new ArrayList<>();
        Function<Description, MavenArtifact> artifactExtractor = d ->
                d.getMetaDescription() != null ? d.getMetaDescription().getMavenArtifactCoordinates() : null;
        if (crDescription != null)
            artifactList.add(Stream.of(artifactExtractor.apply(crDescription)));
        if (cmDelegates != null)
            artifactList.add(cmDelegates.stream().map(artifactExtractor));
        if (aeDelegates != null)
            artifactList.add(aeDelegates.stream().map(artifactExtractor));
        if (ccDelegates != null)
            artifactList.add(ccDelegates.stream().map(artifactExtractor));
        // We filter for null objects because PEAR components don't have a Maven artifact
        return artifactList.stream().flatMap(Function.identity()).filter(Objects::nonNull).filter(a -> Objects.nonNull(a.getArtifactId()));
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

    public Description getAeFlowController() {
        return aeFlowController;
    }

    public void setAeFlowController(Description flowControllerDescription) {
        avoidNamingCollisions(flowControllerDescription);
        this.aeFlowController = flowControllerDescription;
    }

    public Description getCcFlowController() {
        return ccFlowController;
    }

    public void setCcFlowController(Description flowControllerDescription) {
        avoidNamingCollisions(flowControllerDescription);
        this.ccFlowController = flowControllerDescription;
    }
}
