package de.julielab.jcore.pipeline.builder.cli.util;

import de.julielab.jcore.pipeline.builder.base.main.*;
import org.apache.commons.lang.StringUtils;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.resource.*;
import org.apache.uima.resource.metadata.*;
import org.apache.uima.resource.metadata.impl.NameValuePair_impl;
import org.beryx.textio.TextIO;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static de.julielab.jcore.pipeline.builder.cli.menu.TerminalPrefixes.*;
import static de.julielab.jcore.pipeline.builder.cli.util.TextIOUtils.createPrintLine;

public class StatusPrinter {

    public static void printComponentStatus(Description description, boolean brief, TextIO textIO) {
        List<PrintLine> records = getComponentStatusRecords(description, brief);
        TextIOUtils.printLines(records.stream(), textIO);
    }

    public static void printComponentStatus(Description description, TextIO textIO) {
        printComponentStatus(description, true, textIO);
    }

    public static void printComponentMetaData(MetaDescription metaDescription, TextIO textIO) {
        String name = metaDescription.getName();
        ComponentRepository module = metaDescription.getModule();
        //String version = metaDescription.getVersion();
        TextIOUtils.printLines(Stream.of(
                createPrintLine("Component Name: ", FIXED, name, DEFAULT),
                createPrintLine("Component Module: ", FIXED, module.getName(), DEFAULT)//,
                //createPrintLine("Component Version: ", FIXED, version, DEFAULT)
        ), textIO);
    }

    public static void printComponentMetaData(Description description, TextIO textIO) {
        printComponentMetaData(description.getMetaDescription(), textIO);
    }

    private static List<PrintLine> getComponentStatusRecords(Description description, boolean brief) {
        List<PrintLine> records = new ArrayList<>();
        if (!description.getMetaDescription().isPear()) {
            ParameterAdder parameterAdder = new ParameterAdder(records, brief);
            ExternalResourcesAdder externalResourcesAdderAdder = new ExternalResourcesAdder(records);
            parameterAdder.accept(description);
            if (description.getDescriptor() instanceof AnalysisEngineDescription)
                externalResourcesAdderAdder.accept(description.getDescriptorAsAnalysisEngineDescription());
        } else {
            records.add(createPrintLine("  - " + description.getName(), COMPONENT_NAME));
            records.add(createPrintLine("    This is a PEAR and thus not configurable.", PARAM));
            records.add(createPrintLine("    Path: " + description.getLocation(), PARAM));
        }
        return records;
    }

    public static void printPipelineStatus(JCoReUIMAPipeline pipeline, TextIO textIO) {
        printPipelineStatus(pipeline, true, textIO);
    }


    public static void printPipelineStatus(JCoReUIMAPipeline pipeline, boolean brief, TextIO textIO) {
        List<PrintLine> records = getPipelineStatusRecords(pipeline, brief);

        TextIOUtils.printLines(records.stream(), textIO);

        // Useful code for multi-column layout; works only in linux shells
//        int lineLength = 80;
//        try {
//            Process p = Runtime.getRuntime().exec(new String[]{
//                    "bash", "-c", "tput cols 2> /dev/tty"});
//            String response = IOUtils.toString(p.getInputStream()).trim();
//            lineLength = Integer.parseInt(response);
//        } catch (IOException  | NumberFormatException e) {
//            // nothing; may happen, then use the default value
//        }
//        int columnLength = lineLength / 4;
//
//        textIO.getTextTerminal().executeWithPropertiesPrefix(TerminalPrefixes.PIPELINESTATUS_HEADER,
//                t -> t.print("Current pipeline status:" + LS));
//        String colFormat = "%-" + columnLength + "s%-" + columnLength + "s%-" + columnLength + "s%-" + columnLength + "s";
    }

    private static List<PrintLine> getPipelineStatusRecords(JCoReUIMAPipeline pipeline, boolean brief) {
        List<PrintLine> records = new ArrayList<>();
        ParameterAdder parameterAdder = new ParameterAdder(records, brief);
        records.add(createPrintLine("Collection Reader:", HEADER));
        if (pipeline.getCrDescription() != null)
            parameterAdder.accept(pipeline.getCrDescription());
        else
            records.add(createPrintLine("    none", EMPTY));
        if (pipeline.getCmDelegates() != null && !pipeline.getCmDelegates().isEmpty()) {
            records.add(createPrintLine("CAS Multipliers:", HEADER));
            pipeline.getCmDelegates().stream().map(d -> StatusPrinter.getComponentStatusRecords(d, brief)).forEach(records::addAll);
        }
        records.add(createPrintLine("Analysis Engines:", HEADER));
        if (pipeline.getAeDelegates() == null || pipeline.getAeDelegates().isEmpty())
            records.add(createPrintLine("    none", EMPTY));
        else {
            pipeline.getAeDelegates().stream().map(d -> StatusPrinter.getComponentStatusRecords(d, brief)).forEach(records::addAll);
        }

        if (pipeline.getCcDelegates() != null && !pipeline.getCcDelegates().isEmpty()) {
            records.add(createPrintLine("CAS Consumers:", HEADER));
            pipeline.getCcDelegates().stream().map(d -> StatusPrinter.getComponentStatusRecords(d, brief)).forEach(records::addAll);
        }

        // Check if there is an external resource name repeated
        Stream<Description> cmStream = pipeline.getCmDelegates() != null ? pipeline.getCmDelegates().stream() : Stream.empty();
        Stream<Description> aeStream = pipeline.getAeDelegates() != null ? pipeline.getAeDelegates().stream() : Stream.empty();
        Stream<Description> ccStream = pipeline.getCcDelegates() != null ? pipeline.getCcDelegates().stream() : Stream.empty();
        Stream<Description> aes = Stream.concat(Stream.concat(cmStream, aeStream), ccStream).filter(d -> d.getDescriptor() instanceof AnalysisEngineDescription);
        Map<String, List<ExternalResourceDescription>> resourcesByName = aes.map(Description::getDescriptorAsAnalysisEngineDescription).filter(ae -> ae.getResourceManagerConfiguration() != null).filter(ae -> ae.getResourceManagerConfiguration() != null).flatMap(ae -> Stream.of(ae.getResourceManagerConfiguration().getExternalResources())).collect(Collectors.groupingBy(er -> er.getName()));
        resourcesByName.entrySet().stream().filter(e -> e.getValue().size() > 1).forEach(e -> records.add(createPrintLine("Configuration error: There are multiple external resources with the name " + e.getKey() + ".\n    Go to the configuration dialog and adapt the names.", ERROR)));

        return records;
    }


    private static class ParameterAdder implements Consumer<Description> {

        private List<PrintLine> records;
        private boolean brief;

        public ParameterAdder(List<PrintLine> records, boolean brief) {
            this.records = records;
            this.brief = brief;
        }

        @Override
        public void accept(Description description) {
            ResourceMetaData metaData = ((ResourceCreationSpecifier) description.getDescriptor()).getMetaData();
            records.add(createPrintLine("  - " + metaData.getName(), COMPONENT_NAME));
            records.add(createPrintLine("    Maven artifact: " + getArtifactString(description), DEFAULT));
            NameValuePair[] parameterSettings = metaData.getConfigurationParameterSettings().getParameterSettings();
            Set<String> mandatorySet = Stream.of(metaData.getConfigurationParameterDeclarations().getConfigurationParameters()).
                    filter(ConfigurationParameter::isMandatory).map(ConfigurationParameter::getName).collect(Collectors.toSet());
            if ((parameterSettings != null && parameterSettings.length > 0) || !mandatorySet.isEmpty())
                records.add(createPrintLine("    Mandatory Parameters:", HEADER));
            if (parameterSettings != null) {
                for (NameValuePair parameter : parameterSettings) {
                    if (!StringUtils.isBlank(parameter.getValue().toString()) && (mandatorySet.remove(parameter.getName()) || !brief)) {
                        String valueString = parameter.getValue().getClass().isArray() ?
                                Arrays.toString((Object[]) parameter.getValue()) :
                                String.valueOf(parameter.getValue());
                        records.add(createPrintLine("    " + parameter.getName() + ": ", PARAM, valueString, DEFAULT));
                    }
                }
            }
            for (String notSetMandatoryParameter : mandatorySet) {
                records.add(createPrintLine("    " + notSetMandatoryParameter + ": <not set>", ERROR));
            }
        }

        private String getArtifactString(Description description) {
            MavenArtifact artifact = description.getMetaDescription().getMavenArtifact();
            String artifactString = artifact.getGroupId() + ":" + artifact.getArtifactId() + ":" + artifact.getVersion();
            if (artifact.getClassifier() != null)
                artifactString += ":" + artifact.getClassifier();
            return artifactString;
        }
    }

    private static class ExternalResourcesAdder implements Consumer<AnalysisEngineDescription> {

        private List<PrintLine> records;

        public ExternalResourcesAdder(List<PrintLine> records) {
            this.records = records;
        }

        @Override
        public void accept(AnalysisEngineDescription desc) {
            ExternalResourceDependency[] externalResourceDependencies = desc.getExternalResourceDependencies();
            if (externalResourceDependencies != null && externalResourceDependencies.length > 0) {
                records.add(createPrintLine("    External Resources:", HEADER));
                // Show all external resource dependencies by displaying their key and the resource name
                // the key is bound to.
                Set<String> dependenciesToSatisfy = Stream.of(externalResourceDependencies).
                        map(ExternalResourceDependency::getKey).
                        collect(Collectors.toSet());
                ResourceManagerConfiguration resourceManagerConfiguration = desc.getResourceManagerConfiguration();
                Map<String, String> bindingMap = new HashMap<>();
                if (resourceManagerConfiguration != null) {
                    // Get all the binding and build a map from resource dependency key to the name of the resource the
                    // key is bound to.
                    ExternalResourceBinding[] externalResourceBindings = resourceManagerConfiguration.getExternalResourceBindings();
                    bindingMap.putAll(Stream.of(externalResourceBindings).
                            collect(Collectors.toMap(ExternalResourceBinding::getKey, ExternalResourceBinding::getResourceName)));
                }

                Stream.of(externalResourceDependencies).forEach(dependency -> {
                    if (bindingMap.keySet().contains(dependency.getKey())) {
                        String resourceName = bindingMap.get(dependency.getKey());
                        records.add(createPrintLine("    " + dependency.getKey() + ": " + resourceName, DEFAULT));
                        Optional<ExternalResourceDescription> resDescOpt =
                                Stream.of(desc.getResourceManagerConfiguration().getExternalResources()).
                                        filter(res -> res.getName().equals(resourceName)).
                                        findFirst();
                        if (resDescOpt.isPresent()) {
                            ExternalResourceDescription resourceDesc = resDescOpt.get();
                            records.add(createPrintLine("       Name: ", FIXED, resourceDesc.getName(), DEFAULT));
                            records.add(createPrintLine("       Description: ", FIXED, resourceDesc.getDescription(), DEFAULT));
                            records.add(createPrintLine("       Implementation: ", FIXED, resourceDesc.getImplementationName(), DEFAULT));
                            String url = null;
                            ResourceSpecifier resourceSpecifier = resourceDesc.getResourceSpecifier();
                            if (resourceSpecifier instanceof FileResourceSpecifier)
                                url = ((FileResourceSpecifier) resourceSpecifier).getFileUrl();
                            else if (resourceSpecifier instanceof ConfigurableDataResourceSpecifier)
                                url = ((ConfigurableDataResourceSpecifier) resourceSpecifier).getUrl();
                            records.add(createPrintLine("       Resource URL: ", StringUtils.isBlank(url) ? ERROR : FIXED, url, DEFAULT));
                            if (resourceSpecifier instanceof ConfigurableDataResourceSpecifier) {
                                ConfigurableDataResourceSpecifier configurableDataResourceSpecifier = (ConfigurableDataResourceSpecifier) resourceSpecifier;
                                ConfigurationParameter[] declarations = configurableDataResourceSpecifier.
                                        getMetaData().getConfigurationParameterDeclarations().
                                        getConfigurationParameters();
                                Map<String, NameValuePair> settings = Stream.of(configurableDataResourceSpecifier.getMetaData().getConfigurationParameterSettings().getParameterSettings()).flatMap(Stream::of).collect(Collectors.toMap(NameValuePair::getName, Function.identity()));
                                for (int i = 0; i < declarations.length; i++) {
                                    ConfigurationParameter declaration = declarations[i];
                                    String name = declaration.getName();
                                    Object value = Optional.of(settings.get(name)).orElseGet(NameValuePair_impl::new).getValue();
                                    String reportLevel = DEFAULT;
                                    if (declaration.isMandatory() && (value == null || StringUtils.isBlank(value.toString())))
                                        reportLevel = ERROR;
                                    records.add(createPrintLine("       " + name + ": ", PARAM, value.toString(), reportLevel));

                                }
                            }
                        }
                    } else {
                        records.add(createPrintLine("    " + dependency.getKey() + ": <not bound>", ERROR));
                    }
                });
            }
        }
    }
}