package de.julielab.jcore.pipeline.builder.base;

import de.julielab.java.utilities.FileUtilities;
import de.julielab.jcore.pipeline.builder.base.exceptions.PipelineIOException;
import de.julielab.jcore.pipeline.builder.base.main.Description;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import org.apache.commons.lang.StringUtils;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.resource.*;
import org.apache.uima.resource.metadata.*;
import org.apache.uima.resource.metadata.impl.NameValuePair_impl;
import org.apache.uima.util.InvalidXMLException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A helper class to find missing parameters and external resources in a JCoRe UIMA Pipeline.
 */
public class PipelineParameterChecker {
    public static final String MISSING_CONFIG_FILE_NAME = "missing_configuration.txt";

    private PipelineParameterChecker() {
    }

    public static void main(String args[]) throws PipelineIOException, IOException {
        if (args.length != 1) {
            System.out.println("Usage: " + PipelineParameterChecker.class.getSimpleName() + " <root directory of the JCoRe pipeline>");
            System.exit(1);
        }
        File pipelineRoot = new File(args[0]);
        File outputFile = new File(pipelineRoot.getAbsolutePath() + File.separator + MISSING_CONFIG_FILE_NAME);
        JCoReUIMAPipeline jCoReUIMAPipeline = new JCoReUIMAPipeline(pipelineRoot);
        jCoReUIMAPipeline.load(false);
        writeMissingConfigurationToFile(jCoReUIMAPipeline, outputFile);
    }

    public static List<MissingComponentConfiguration> findMissingParameters(ResourceMetaData metaData) {
        List<MissingComponentConfiguration> list = new ArrayList<>();

        NameValuePair[] parameterSettings = metaData.getConfigurationParameterSettings().getParameterSettings();
        Set<String> mandatorySet = Stream.of(metaData.getConfigurationParameterDeclarations().getConfigurationParameters()).
                filter(ConfigurationParameter::isMandatory).map(ConfigurationParameter::getName).collect(Collectors.toSet());
        if (parameterSettings != null) {
            for (NameValuePair parameter : parameterSettings) {
                if (!StringUtils.isBlank(parameter.getValue().toString()))
                    mandatorySet.remove(parameter.getName());
            }
        }
        for (String notSetMandatoryParameter : mandatorySet) {
            list.add(new MissingComponentConfiguration(Missing.PARAMETER, metaData.getName(), notSetMandatoryParameter));
        }
        return list;
    }

    public static List<MissingComponentConfiguration> findMissingResources(ResourceCreationSpecifier desc) {
        List<MissingComponentConfiguration> list = new ArrayList<>();
        ExternalResourceDependency[] externalResourceDependencies = desc.getExternalResourceDependencies();
        if (externalResourceDependencies != null && externalResourceDependencies.length > 0) {
            ResourceManagerConfiguration resourceManagerConfiguration = desc.getResourceManagerConfiguration();
            Map<String, String> bindingMap = new HashMap<>();
            if (resourceManagerConfiguration != null) {
                // Get all the binding and build a map from resource dependency key to the parameterName of the resource the
                // key is bound to.
                ExternalResourceBinding[] externalResourceBindings = resourceManagerConfiguration.getExternalResourceBindings();
                bindingMap.putAll(Stream.of(externalResourceBindings).
                        collect(Collectors.toMap(ExternalResourceBinding::getKey, ExternalResourceBinding::getResourceName)));
            }

            Stream.of(externalResourceDependencies).forEach(dependency -> {
                if (bindingMap.keySet().contains(dependency.getKey())) {
                    String resourceName = bindingMap.get(dependency.getKey());
                    Optional<ExternalResourceDescription> resDescOpt =
                            Stream.of(desc.getResourceManagerConfiguration().getExternalResources()).
                                    filter(res -> res.getName().equals(resourceName)).
                                    findFirst();
                    if (resDescOpt.isPresent()) {
                        ExternalResourceDescription resourceDesc = resDescOpt.get();
                        ResourceSpecifier resourceSpecifier = resourceDesc.getResourceSpecifier();
                        if (resourceSpecifier instanceof ConfigurableDataResourceSpecifier) {
                            ConfigurableDataResourceSpecifier configurableDataResourceSpecifier = (ConfigurableDataResourceSpecifier) resourceSpecifier;
                            ConfigurationParameter[] declarations = configurableDataResourceSpecifier.
                                    getMetaData().getConfigurationParameterDeclarations().
                                    getConfigurationParameters();
                            // Map the values to their names so for each parameter declaration we can quickly check the value
                            Map<String, NameValuePair> settings = Stream.of(configurableDataResourceSpecifier.getMetaData().getConfigurationParameterSettings().getParameterSettings()).flatMap(Stream::of).collect(Collectors.toMap(NameValuePair::getName, Function.identity()));
                            for (int i = 0; i < declarations.length; i++) {
                                ConfigurationParameter declaration = declarations[i];
                                String name = declaration.getName();
                                Object value = Optional.ofNullable(settings.get(name)).orElseGet(NameValuePair_impl::new).getValue();
                                if (declaration.isMandatory() && (value == null || StringUtils.isBlank(value.toString())))
                                    list.add(new MissingComponentConfiguration(Missing.EXTERNAL_RESOURCE_PARAMETER, desc.getMetaData().getName(), dependency.getKey()));

                            }
                        }
                    }
                } else {
                    list.add(new MissingComponentConfiguration(Missing.EXTERNAL_RESOURCE, desc.getMetaData().getName(), dependency.getKey()));
                }
            });
        }
        return list;
    }

    public static List<MissingComponentConfiguration> findMissingConfiguration(JCoReUIMAPipeline pipeline) {
        List<MissingComponentConfiguration> list = new ArrayList<>();
        if (pipeline.getCrDescription() == null)
            list.add(new MissingComponentConfiguration(Missing.READER));
        else
            list.addAll(findMissingParameters(pipeline.getCrDescription().getDescriptorAsCollectionReaderDescription().getMetaData()));
        if (pipeline.getAaeDesc() == null && pipeline.getCcDesc() == null)
            list.add(new MissingComponentConfiguration(Missing.AE_AND_CC));
        else if (pipeline.getAaeDesc() != null) {
            try {
                pipeline.getAaeDesc().getDelegateAnalysisEngineSpecifiers().values().stream().
                        // PEAR descriptors are not instances of AnalysisEngineDescription.
                        // Since we cannot configure them anyway, filter them out
                        filter(AnalysisEngineDescription.class::isInstance).
                        map(AnalysisEngineDescription.class::cast).
                        map(AnalysisEngineDescription::getAnalysisEngineMetaData).
                        map(PipelineParameterChecker::findMissingParameters).
                        flatMap(Collection::stream).forEach(list::add);
                pipeline.getAaeDesc().getDelegateAnalysisEngineSpecifiers().values().stream().
                        filter(AnalysisEngineDescription.class::isInstance).
                        map(AnalysisEngineDescription.class::cast).
                        map(PipelineParameterChecker::findMissingResources).
                        flatMap(Collection::stream).forEach(list::add);
            } catch (InvalidXMLException e) {
                list.add(new MissingComponentConfiguration(Missing.PARSING_ERROR, "<AAE delegate>", "Could not parse an AAE delegate specifier: " + e.getMessage()));
            }
        }
        if (pipeline.getCcDelegates() != null && !pipeline.getCcDelegates().isEmpty()) {
            if (pipeline.getCcDelegates().size() == 1) {
                list.addAll(findMissingParameters(((ResourceCreationSpecifier)pipeline.getCcDelegates().get(0).getDescriptor()).getMetaData()));
                list.addAll(findMissingResources((ResourceCreationSpecifier)pipeline.getCcDelegates().get(0).getDescriptor()));
            } else {
                pipeline.getCcDelegates().stream().
                        map(Description::getDescriptor).
                        map(ResourceCreationSpecifier.class::cast).
                        map(ResourceCreationSpecifier::getMetaData).
                        map(PipelineParameterChecker::findMissingParameters).
                        flatMap(Collection::stream).forEach(list::add);
                pipeline.getCcDelegates().stream().
                        map(Description::getDescriptor).
                        map(AnalysisEngineDescription.class::cast).
                        map(PipelineParameterChecker::findMissingResources).
                        flatMap(Collection::stream).forEach(list::add);
            }
        }

        // Check if there is an external resource name repeated
        Stream<Description> cmStream = pipeline.getCmDelegates() != null ? pipeline.getCmDelegates().stream() : Stream.empty();
        Stream<Description> aeStream = pipeline.getAeDelegates() != null ? pipeline.getAeDelegates().stream() : Stream.empty();
        Stream<Description> ccStream = pipeline.getCcDelegates() != null ? pipeline.getCcDelegates().stream() : Stream.empty();
        Stream<Description> aes = Stream.concat(Stream.concat(cmStream, aeStream), ccStream).filter(d -> d.getDescriptor() instanceof AnalysisEngineDescription);
        Map<String, List<ExternalResourceDescription>> resourcesByName = aes.map(Description::getDescriptorAsAnalysisEngineDescription).filter(ae -> ae.getResourceManagerConfiguration() != null).filter(ae -> ae.getResourceManagerConfiguration().getExternalResources() != null).flatMap(ae -> Stream.of(ae.getResourceManagerConfiguration().getExternalResources())).collect(Collectors.groupingBy(er -> er.getName()));
        resourcesByName.entrySet().stream().filter(e -> e.getValue().size() > 1).forEach(e -> list.add(new MissingComponentConfiguration(Missing.DUPLICATE_EXTERNAL_RESOURCE_NAME, null, e.getKey()) ));
        return list;
    }

    public static void writeMissingConfigurationToFile(JCoReUIMAPipeline pipeline, File file) throws IOException {
        List<MissingComponentConfiguration> missingConfiguration = findMissingConfiguration(pipeline);
        if (!missingConfiguration.isEmpty())
            writeMissingConfigurationToFile(missingConfiguration, file);

    }

    public static void writeMissingConfigurationToFile(List<MissingComponentConfiguration> missingConfiguration, File file) throws IOException {
        try (BufferedWriter w = FileUtilities.getWriterToFile(file)) {
            for (MissingComponentConfiguration missingConfig : missingConfiguration) {
                if (missingConfig.getMissingItem() == Missing.READER) {
                    w.write("The pipeline has no collection reader.");
                    w.newLine();
                    w.newLine();
                }
                if (missingConfig.getMissingItem() == Missing.AE_AND_CC) {
                    w.write("The pipeline has no analysis engine and no consumers.");
                    w.newLine();
                    w.newLine();
                }
                if (missingConfig.getMissingItem() == Missing.PARAMETER) {
                    w.write("Component name: " + missingConfig.getComponentName());
                    w.newLine();
                    w.write("The mandatory parameter \"" + missingConfig.getParameterName() + "\" is not set.");
                    w.newLine();
                    w.newLine();
                }
                if (missingConfig.getMissingItem() == Missing.EXTERNAL_RESOURCE) {
                    w.write("Component name: " + missingConfig.getComponentName());
                    w.newLine();
                    w.write("The mandatory external resource dependency with key \"" + missingConfig.getParameterName() + "\" is not bound to a resource.");
                    w.newLine();
                    w.newLine();
                }
                if (missingConfig.getMissingItem() == Missing.EXTERNAL_RESOURCE_PARAMETER) {
                    w.write("External resource with name: " + missingConfig.getComponentName());
                    w.newLine();
                    w.write("The mandatory parameter \"" + missingConfig.getParameterName() + "\" is not set on the resource.");
                    w.newLine();
                    w.newLine();
                }
                if (missingConfig.getMissingItem() == Missing.PARSING_ERROR) {
                    w.write("Could not reader \"" + missingConfig.getComponentName() + "\"due to a parsing error: " + missingConfig.getParameterName());
                    w.newLine();
                    w.newLine();
                }
                if (missingConfig.getMissingItem() == Missing.DUPLICATE_EXTERNAL_RESOURCE_NAME) {
                    w.write("There are multiple external resources with name \"" + missingConfig.getParameterName() + "\". UIMA will only use the first definition.");
                    w.newLine();
                    w.newLine();
                }
            }
        }
    }

    public enum Missing {READER, AE_AND_CC, PARAMETER, EXTERNAL_RESOURCE, EXTERNAL_RESOURCE_PARAMETER, PARSING_ERROR, DUPLICATE_EXTERNAL_RESOURCE_NAME}

    public static class MissingComponentConfiguration {
        private Missing missingItem;
        private String componentName;
        private String parameterName;

        public MissingComponentConfiguration(Missing missingItem) {
            this.missingItem = missingItem;
        }

        public MissingComponentConfiguration(Missing missingItem, String componentName, String parameterName) {
            this.missingItem = missingItem;
            this.componentName = componentName;
            this.parameterName = parameterName;
        }

        public Missing getMissingItem() {
            return missingItem;
        }

        public void setMissingItem(Missing missingItem) {
            this.missingItem = missingItem;
        }

        public String getComponentName() {
            return componentName;
        }

        public void setComponentName(String componentName) {
            this.componentName = componentName;
        }

        public String getParameterName() {
            return parameterName;
        }

        public void setParameterName(String parameterName) {
            this.parameterName = parameterName;
        }
    }

}
