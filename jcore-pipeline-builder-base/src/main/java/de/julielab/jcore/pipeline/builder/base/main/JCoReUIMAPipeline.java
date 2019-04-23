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
import org.apache.uima.collection.metadata.CpeDescription;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
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
    private static final String SERIALIZED_CR_DESCS_FILE = "crDescriptions.json";
    private static final String SERIALIZED_CM_DESCS_FILE = "cmDescriptions.json";
    private static final String SERIALIZED_AE_DESCS_FILE = "aeDescriptions.json";
    private static final String SERIALIZED_CC_DESCS_FILE = "ccDescriptions.json";
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
    /**
     * The parent POM is used for dependency resolution of the component artifacts. It can be used to resolve
     * library version conflicts using the dependencyManagement mechanism. May be <tt>null</tt>.
     */
    private MavenArtifact parentPom;
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
        this.crDescription = crDescription;
        avoidNamingCollisions(crDescription);
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
            if (aeDelegates.stream().filter(Description::isActive).count() > 0) {
                Stream<AnalysisEngineDescription> descStream = aeDelegates.stream().
                        // filter(desc -> !desc.getMetaDescription().isPear()).
                                filter(Description::isActive).
                                map(Description::getDescriptorAsAnalysisEngineDescription);
                aaeDesc = createAAE(descDir, "AggregateAnalysisEngine", aeDelegates, descStream, true);
            }
            // Create the CM AAE, if required
            if (cmDelegates != null && cmDelegates.stream().filter(Description::isActive).count() > 1) {
                Stream<AnalysisEngineDescription> descStream = cmDelegates.stream().filter(Description::isActive).
                        map(Description::getDescriptorAsAnalysisEngineDescription);
                aaeCmDesc = createAAE(descDir, "AggregateMultiplier", cmDelegates, descStream, true);
            }
            File crFile = null;
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
            } else if (aaeFile.exists()) {
                aaeFile.delete();
            }

            File ccFile = new File(descDir.getAbsolutePath() +
                    File.separator +
                    "AggregateConsumer.xml");
            if (ccFile.exists())
                ccFile.delete();
            if (ccDelegates != null && ccDelegates.stream().filter(Description::isActive).count() != 0) {
                final List<Description> activeCCs = ccDelegates.stream().filter(Description::isActive).collect(Collectors.toList());
                for (Description ccDesc : activeCCs) {
                    storeCCDescriptor(ccDesc, descDir);
                }
                if (activeCCs.size() == 1) {
                    final Description activeCCDescription = activeCCs.get(0);
                    ccDesc = (ResourceCreationSpecifier) activeCCDescription.getDescriptor();
                    ccFile = new File(descDir.getAbsolutePath() + File.separator + activeCCs.get(0).getUimaDescPath());
                } else if (activeCCs.size() > 1) {
                    ccDesc = createAAE(descDir, "AggregateConsumer", ccDelegates, activeCCs.stream().map(Description::getDescriptorAsAnalysisEngineDescription), true);
                    ccDesc.getMetaData().setName("JCoRe Consumer AAE");
                    ccDesc.getMetaData().setDescription("This consumer AAE descriptor directly contains the CAS consumers added " +
                            "through the JCoRe pipeline builder. The AAE serves to bundle all the components together.");

                    // And for the actual active pipeline descriptors directory
                    storeDescriptor(ccDesc, ccFile);
                }
            }

            // Storing a CPE descriptor
            try {
                final File cpeAAEFile = new File(descDir.getAbsolutePath() + File.separator + CPE_AAE_DESC_NAME);
                final File cpeFile = new File(descDir.getAbsolutePath() + File.separator +
                        "CPE.xml");
                if (ccDelegates == null || ccDelegates.stream().map(Description::getDescriptor).filter(CasConsumer.class::isInstance).count() == 0) {

                    final CPE cpe = new CPE();
                    if (crDescription != null) {
                        cpe.setCollectionReader(crDescription);
                    }
                    AnalysisEngineDescription cpeAAE = AnalysisEngineFactory.createEngineDescription();

                    boolean multipleDeploymentAllowed = true;
                    cpeAAE.getDelegateAnalysisEngineSpecifiersWithImports().clear();
                    if (cmDelegates.stream().filter(Description::isActive).count() > 0) {
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
                    if (ccDelegates != null && ccDelegates.stream().filter(Description::isActive).count() > 0) {
                        Import_impl ccImport = new Import_impl();
                        ccImport.setLocation(ccFile.getName());
                        ccImport.setSourceUrl(ccFile.toURI().toURL());
                        cpeAAE.getDelegateAnalysisEngineSpecifiersWithImports().put(ccDesc.getMetaData().getName(), ccImport);
                        boolean ccMultipleDeploymentAllowed = true;
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
                    if (ccDelegates != null && ccDelegates.stream().filter(Description::isActive).count() > 0)
                        descriptorsForFlow = Stream.concat(descriptorsForFlow, Stream.of(ccDesc));
                    String[] flow = descriptorsForFlow.filter(Objects::nonNull).map(ResourceCreationSpecifier::getMetaData).map(ResourceMetaData::getName).toArray(String[]::new);
                    ((FixedFlow) cpeAAE.getAnalysisEngineMetaData().getFlowConstraints()).setFixedFlow(flow);
                    cpeAAE.toXML(
                            FileUtilities.getWriterToFile(
                                    cpeAAEFile));
                    cpe.setAnalysisEngine(cpeAAEFile.getName(), "CPE AAE");
                    // Modify the CollectionReader part of the CPE to just refer to the Collection Reader descriptor file
                    // by relative location instead of absolute URLs.
//                    if (crDescription != null) {
//                        Import_impl crImport = new Import_impl();
//                        crImport.setLocation(crFile.getName());
//                        final CpeComponentDescriptor crDescriptor = cpeDescription.getAllCollectionCollectionReaders()[0].getCollectionIterator().
//                                getDescriptor();
//                        // delete the automatically generated include; we don't want an include (Absolute URLs are used)
//                        // but an import by location (a path relative to the CPE.xml descriptor is used)
//                        crDescriptor.setInclude(null);
//                        crDescriptor.setImport(crImport);
//                    }

                    // Now create a single import referencing the CPE AAE XML file. UIMAfit would just write
                    // the complete markup of all the components of the CPE AAE into the CPE descriptor.
                    // This has two issues:
                    // 1. CAS multipliers seem only to work within an aggregate
                    // 2. If we modify one of the descriptor files, the CPE won't reflect the change.
//                    Import_impl cpeAaeImport = new Import_impl();
//                    cpeAaeImport.setLocation(cpeAAEFile.getName());
//                    CpeComponentDescriptorImpl cpeComponentDescriptor = new CpeComponentDescriptorImpl();
//                    cpeComponentDescriptor.setImport(cpeAaeImport);
//                    CpeIntegratedCasProcessorImpl cpeIntegratedCasProcessor = new CpeIntegratedCasProcessorImpl();
//                    cpeIntegratedCasProcessor.setCpeComponentDescriptor(cpeComponentDescriptor);
//                    cpeIntegratedCasProcessor.setName("CPE AAE");
//                    cpeIntegratedCasProcessor.setBatchSize(500);
//                    CpeCasProcessorsImpl cpeCasProcessors = new CpeCasProcessorsImpl();
//                    cpeCasProcessors.addCpeCasProcessor(cpeIntegratedCasProcessor);

                    final CpeDescription cpeDescription = cpe.getDescription();
//                    cpeDescription.setCpeCasProcessors(cpeCasProcessors);
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

    // I'm actually not sure why we need 'allDelegates' and 'aaeElements'. Perhaps we don't. Try it when there is time.
    private AnalysisEngineDescription createAAE(File descDir, String name, List<Description> allDelegates, Stream<AnalysisEngineDescription> aaeElements, boolean filterDeactivated) throws ResourceInitializationException, SAXException, IOException, InvalidXMLException, PipelineIOException {
        AnalysisEngineDescription aaeDesc = AnalysisEngineFactory.createEngineDescription(aaeElements.toArray(AnalysisEngineDescription[]::new));
        Map<String, MetaDataObject> delegatesWithImports = aaeDesc.getDelegateAnalysisEngineSpecifiersWithImports();
        delegatesWithImports.clear();
        List<String> flowNames = new ArrayList<>();
        boolean multipleDeploymentAllowed = true;
        for (int i = 0; i < allDelegates.size(); ++i) {
            Description description = allDelegates.get(i);
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
            getClasspathElements().forEach(JarLoader::addJarToClassPath);
            aaeDesc.getDelegateAnalysisEngineSpecifiers();
        }
        ((FixedFlow) aaeDesc.getAnalysisEngineMetaData().getFlowConstraints()).setFixedFlow(flowNames.toArray(new String[flowNames.size()]));
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
        for (Description aeDelegate : aeDelegates)
            storeDescriptor(aeDelegate.getDescriptorAsAnalysisEngineDescription(), getDescriptorStoragePath(aeDelegate, descDirAll).toFile());
        for (Description ccDelegate : ccDelegates)
            storeCCDescriptor(ccDelegate, descDirAll);

        // Store AAEs
        if (!cmDelegates.isEmpty() && cmDelegates.size() > 1) {
            final AnalysisEngineDescription aae = createAAE(descDirAll, "AggregateMultiplier", cmDelegates, cmDelegates.stream().map(Description::getDescriptorAsAnalysisEngineDescription), false);
            storeDescriptor(aae, Paths.get(descDirAll.getAbsolutePath(), aae.getMetaData().getName() + ".xml").toFile());
        }
        if (!aeDelegates.isEmpty() && aeDelegates.size() > 1) {
            final AnalysisEngineDescription aae = createAAE(descDirAll, "AggregateAnalysisEngine", aeDelegates, aeDelegates.stream().map(Description::getDescriptorAsAnalysisEngineDescription), false);
            storeDescriptor(aae, Paths.get(descDirAll.getAbsolutePath(), aae.getMetaData().getName() + ".xml").toFile());
        }
        if (!ccDelegates.isEmpty() && ccDelegates.size() > 1) {
            final AnalysisEngineDescription aae = createAAE(descDirAll, "AggregateConsumer", ccDelegates, ccDelegates.stream().map(Description::getDescriptorAsAnalysisEngineDescription), false);
            storeDescriptor(aae, Paths.get(descDirAll.getAbsolutePath(), aae.getMetaData().getName() + ".xml").toFile());
        }

    }

    private void storeDescriptor(ResourceCreationSpecifier spec, File path) throws IOException, SAXException {
        spec.setSourceUrl(path.toURI().toURL());
        try (final BufferedWriter writer = FileUtilities.getWriterToFile(path)) {
            if (spec instanceof AnalysisEngineDescription)
                ((AnalysisEngineDescription) spec).toXML(writer, true);
            else
                spec.toXML(writer);
        }
    }

    private Path getDescriptorStoragePath(Description desc, File destinationDir) {
        final String filename = desc.getName() + ".xml";
        Path path = Paths.get(destinationDir.getAbsolutePath(), filename);
        return path;
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

//        File libDir = new File(directory.getAbsolutePath() + File.separator + DIR_LIB);
//        if (!libDir.exists())
//            libDir.mkdirs();
//        if (crDescription != null && crDescription.getMetaDescription() != null) {
//            AetherUtilities.storeArtifactWithDependencies(crDescription.getMetaDescription().getMavenArtifact(), libDir);
//        }
//        Consumer<Description> storeArtifacts = desc -> {
//            try {
//                AetherUtilities.storeArtifactWithDependencies(desc.getMetaDescription().getMavenArtifact(), libDir);
//            } catch (MavenException e) {
//                log.error("Could not store dependencies of description {}", desc, e);
//            }
//        };
//        if (cmDelegates != null)
//            cmDelegates.stream().filter(d -> Objects.nonNull(d.getMetaDescription())).forEach(storeArtifacts);
//        if (aeDelegates != null)
//            aeDelegates.stream().filter(d -> !d.getMetaDescription().isPear() && Objects.nonNull(d.getMetaDescription())).forEach(storeArtifacts);
//        if (ccDelegates != null)
//            ccDelegates.stream().filter(d -> Objects.nonNull(d.getMetaDescription())).forEach(storeArtifacts);
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

    private <T> T deserializeDescriptions(File pipelineStorageDir, String sourceFileName, TypeReference<?> typeRef) throws IOException, ClassNotFoundException {
        File sourceFile = new File(pipelineStorageDir.getAbsolutePath() + File.separatorChar + sourceFileName);
        if (!sourceFile.exists())
            return null;
        final ObjectMapper om = new ObjectMapper();
        try (BufferedReader reader = FileUtilities.getReaderFromFile(sourceFile)) {
            return om.readValue(reader, typeRef);
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
                    aeDelegates = deserializeDescriptions(loadDirectory, SERIALIZED_AE_DESCS_FILE, new TypeReference<List<Description>>() {
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


            // the descriptions are loaded at the beginning of the method, if existent
            // When accessing aggregate engine delegates, their types are resolved. Thus, we first need to load
            // the libraries of the pipeline
            long time = System.currentTimeMillis();
            //getClasspathElements().forEach(JarLoader::addJarToClassPath);
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
            try {
                if (crDescription != null && !crDescs.isEmpty())
                    crDescription.setDescriptor(crDescs.get(0));
                if (aaeCmDesc != null && !cmDelegates.isEmpty())
                    setAaeDescriptors(aaeCmDesc, cmDelegates, "CAS Multiplier");
                if (aaeDesc != null && !aeDelegates.isEmpty())
                    setAaeDescriptors(aaeDesc, aeDelegates, "Analysis Engine");
                if (ccDesc != null && !ccDelegates.isEmpty() && ccDesc instanceof AnalysisEngineDescription)
                    setAaeDescriptors((AnalysisEngineDescription) ccDesc, ccDelegates, "CAS Consumer");
            } catch (Exception e) {
                log.warn("Could not set descriptor files from the {}/ directory to the serialized meta descriptions. Changes in the descriptors that have not been stored in the meta descriptions won't be available.", DIR_DESC, e);
            }
        } catch (IOException | InvalidXMLException | URISyntaxException | ResourceInitializationException e) {
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
            Map<String, Description> descByName = descriptions.stream().collect(Collectors.toMap(Description::getName, Function.identity()));
            FlowConstraints flowConstraints = aae.getAnalysisEngineMetaData().getFlowConstraints();
            if (!(flowConstraints instanceof FixedFlow))
                throw new PipelineIOException(String.format("The %s aggregate does not define a FixedFlow. Only FixedFlow constraints are currently supported.", type));
            FixedFlow flow = (FixedFlow) flowConstraints;
            for (int i = 0; i < flow.getFixedFlow().length; ++i) {
                String component = flow.getFixedFlow()[i];
                ResourceSpecifier descriptor;
                try {
                    descriptor = aae.getDelegateAnalysisEngineSpecifiers().get(component);
                } catch (InvalidXMLException e) {
                    // This exception can happen because the types of the AAE are missing. Try to load all JAR with "types" in them
                    // We also load "splitter" because this applies to the jcore-xmi-splitter which has its own types
                    // Obviously, some better mechanism could be of use here.
                    getClasspathElements().filter(f -> f.getName().contains("types") || f.getName().contains("splitter")).forEach(JarLoader::addJarToClassPath);
                    try {
                        descriptor = aae.getDelegateAnalysisEngineSpecifiers().get(component);
                    } catch (InvalidXMLException e1) {
                        // Still no luck, load everything.
                        getClasspathElements().forEach(JarLoader::addJarToClassPath);
                        descriptor = aae.getDelegateAnalysisEngineSpecifiers().get(component);
                    }
                }
                if (i < descriptions.size()) {
                    if (!descByName.containsKey(component))
                        throw new IllegalStateException("The " + type + " AAE specifies the component key " + component + " but no descriptor has this name. The descriptor names and the AAE keys must match.");
                    descByName.get(component).setDescriptor(descriptor);
                }

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
            getClasspathElements().forEach(JarLoader::addJarToClassPath);
            delegateUris = new HashSet<>();
            for (AnalysisEngineDescription aaeDesc : aaeDescs) {
                delegateUris.addAll(aaeDesc.getDelegateAnalysisEngineSpecifiers().values().stream().map(ResourceSpecifier::getSourceUrl).map(url2String).collect(toList()));
            }
        }
        Sets.SetView<String> topAAEs = Sets.difference(aaeUris, delegateUris);
        Sets.SetView<String> aesNotInAAE = Sets.difference(aeUris, delegateUris);

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
            List<String> flowNames = Arrays.asList(aeDesc.getName());

            aaePear.getAnalysisEngineMetaData().setFlowConstraints(new FixedFlow_impl());
            ((FixedFlow) aaePear.getAnalysisEngineMetaData().getFlowConstraints()).setFixedFlow(flowNames.toArray(new String[flowNames.size()]));
            try {
                aaePear.getDelegateAnalysisEngineSpecifiers();
                aeDesc = aeDesc.clone();
                aeDesc.setDescriptor(aaePear);
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            } catch (InvalidXMLException e) {
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
        aeDelegates.stream().map(Description::getName).forEach(names::add);
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
            if (anyExistingArtifactForComponent.isPresent()) {
                description.getMetaDescription().getMavenArtifact().setVersion(anyExistingArtifactForComponent.get().getVersion());
            }
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
        return artifactList.stream().flatMap(Function.identity()).filter(Objects::nonNull);
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
