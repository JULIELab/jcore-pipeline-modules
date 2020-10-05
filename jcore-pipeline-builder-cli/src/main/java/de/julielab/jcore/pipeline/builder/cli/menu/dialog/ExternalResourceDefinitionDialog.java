package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.exceptions.PipelineIOException;
import de.julielab.jcore.pipeline.builder.base.main.Description;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.builder.cli.menu.TerminalPrefixes;
import de.julielab.jcore.pipeline.builder.cli.util.ArtifactLoader;
import de.julielab.jcore.pipeline.builder.cli.util.MenuItemExecutionException;
import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import org.apache.commons.lang3.StringUtils;
import org.apache.uima.fit.factory.ExternalResourceFactory;
import org.apache.uima.resource.ExternalResourceDependency;
import org.apache.uima.resource.ExternalResourceDescription;
import org.apache.uima.resource.ResourceCreationSpecifier;
import org.apache.uima.resource.SharedResourceObject;
import org.beryx.textio.TextIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ExternalResourceDefinitionDialog implements IMenuDialog {
    private final static Logger log = LoggerFactory.getLogger(ExternalResourceDefinitionDialog.class);
    private final JCoReUIMAPipeline pipeline;
    private final Description description;

    public ExternalResourceDefinitionDialog(JCoReUIMAPipeline pipeline, Description description) {
        this.pipeline = pipeline;
        this.description = description;
    }

    @Override
    public String getName() {
        return null;
    }

    public void createExternalResourceBinding(TextIO textIO, ExternalResourceDependency dependency) throws MenuItemExecutionException {
        String LS = System.getProperty("line.separator");
        textIO.getTextTerminal().
                print(String.format("Define an external resource for the resource dependency %s.%s" +
                        "Description: %s%s", dependency.getKey(), LS, dependency.getDescription(), LS));
        String sharedResourceObjectClass = null;
        String dependencyInterfaceName = dependency.getInterfaceName();
        try {
            textIO.getTextTerminal().println("Loading the libraries of the pipeline in order to find eligible " +
                    "external resource implementations for the resource interface " + dependencyInterfaceName + "...");
            if (log.isTraceEnabled())
                pipeline.getClasspathElements().forEach(e -> log.trace("Loading classpath element {}", e));
            pipeline.getClasspathElements().forEach(ArtifactLoader::loadArtifact);
            if (!StringUtils.isBlank(dependencyInterfaceName)) {
                Class<?> forName = Class.forName(dependencyInterfaceName);
                FastClasspathScanner fcs = new FastClasspathScanner();
                List<Object> list = new ArrayList<>();
                fcs.matchClassesImplementing(forName, list::add);
                textIO.getTextTerminal().printf("Searching for implementations of the external resource dependency " +
                        "interface %s...", dependencyInterfaceName);
                fcs.scan();
                list.add("<class not listed>");
                Object response = textIO.newGenericInputReader(null).withNumberedPossibleValues(list).read("Select an implementation " +
                        "of the external resource interface.");
                if (response.toString().equals("<class not listed>")) {
                    sharedResourceObjectClass = textIO.newStringInputReader().read("Enter the implementation name. It must be on the classpath " +
                            "when the pipeline is run.");
                } else {
                    sharedResourceObjectClass = ((Class<?>) response).getCanonicalName();
                }
            }
            String resourceUrl = textIO.newStringInputReader().read("Specify the URL pointing to the external resource:");
            String resourceName = textIO.newStringInputReader().read("Specify a human readable resource name:");
            String resourceDescription = textIO.newStringInputReader().read("Specify a description for the resource:");
            Class<? extends SharedResourceObject> implementationClass = null;
            while (implementationClass == null && !StringUtils.isBlank(dependencyInterfaceName)) {
                try {
                    implementationClass = (Class<? extends SharedResourceObject>) Class.forName(sharedResourceObjectClass);
                } catch (ClassNotFoundException e) {
                    String message = "The resource implementation class " + sharedResourceObjectClass + " could not be found.";
                    textIO.getTextTerminal().executeWithPropertiesPrefix(TerminalPrefixes.ERROR, t -> t.print(message));
                    sharedResourceObjectClass = textIO.newStringInputReader().withMinLength(0).read("Provide a new class name of hit enter to abort:");
                    if (StringUtils.isBlank(sharedResourceObjectClass))
                        return;
                }
            }
            ExternalResourceDescription externalResourceDescription = ExternalResourceFactory.createExternalResourceDescription(resourceName, implementationClass, resourceUrl);
            externalResourceDescription.setDescription(resourceDescription);
            ExternalResourceFactory.bindExternalResource((ResourceCreationSpecifier) description.getDescriptor(), dependency.getKey(), externalResourceDescription);
            clearTerminal(textIO);

        } catch (ClassNotFoundException | PipelineIOException e) {
            throw new MenuItemExecutionException(e);
        }
    }

    @Override
    public String toString() {
        return getName();
    }
}
