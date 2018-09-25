package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.main.Description;
import de.julielab.jcore.pipeline.builder.base.main.JCoReUIMAPipeline;
import de.julielab.jcore.pipeline.builder.cli.menu.*;
import de.julielab.jcore.pipeline.builder.cli.util.MenuItemExecutionException;
import de.julielab.jcore.pipeline.builder.cli.util.StatusPrinter;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.resource.ExternalResourceDependency;
import org.apache.uima.resource.ExternalResourceDescription;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.impl.ExternalResourceDependency_impl;
import org.apache.uima.resource.metadata.ExternalResourceBinding;
import org.beryx.textio.TextIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Deque;
import java.util.Optional;
import java.util.stream.Stream;

public class DescriptorConfigurationDialog implements ILoopablePipelineManipulationDialog {
    private final static Logger log = LoggerFactory.getLogger(DescriptorConfigurationDialog.class);
    private MenuItemList<IMenuItem> itemList;
    private Description description;

    public DescriptorConfigurationDialog(Description description) {
        this.description = description;
        createMenuItems(description);
    }

    private void createMenuItems(Description description) {
        itemList = new MenuItemList<>();
        // Add a menu item to change the component name
        IMenuItem nameItem = new IMenuItem() {
            @Override
            public String getName() {
                return "<Component Name>";
            }

            @Override
            public String toString() {
                return getName();
            }
        };
        itemList.add(nameItem);
        // Add menu items for the parameters of the component
        description.getConfigurationParameters().values().stream().
                map(p -> {
                    if (p.isMultiValued())
                        return new MultiValuedParameterEditingMenuItem(description, p);
                    return new ParameterEditingMenuItem((ResourceSpecifier) description.getDescriptor(), p);
                }).
                forEach(itemList::add);
        Collections.sort(itemList);
        // Add menu items for the external resource bindings of the component
        MenuItemList<IMenuItem> externalResourceItems = new MenuItemList<>();
        if (description.getDescriptor() instanceof AnalysisEngineDescription) {
            ExternalResourceDependency[] externalResourceDependencies = description.getDescriptorAsAnalysisEngineDescription().getExternalResourceDependencies();
            Stream.of(externalResourceDependencies).
                    map(ExternalResourceEditingMenuItem::new).
                    forEach(externalResourceItems::add);
            Collections.sort(externalResourceItems);
            itemList.addAll(externalResourceItems);

        }
        if (description.getDescriptor() instanceof AnalysisEngineDescription)
            itemList.add(new ExternalResourceDependencyDefinitionMenuItem());
        itemList.add(new BackMenuItem());
    }

    @Override
    public String getName() {
        return "Configure Descriptor";
    }

    @Override
    public IMenuItem executeMenuItem(JCoReUIMAPipeline pipeline, TextIO textIO, Deque<String> path) {
        printPosition(textIO, path);
        StatusPrinter.printComponentStatus(description, false, textIO);
        StatusPrinter.printComponentMetaData(description, textIO);
        IMenuItem choice = textIO.<IMenuItem>newGenericInputReader(null)
                .withNumberedPossibleValues(itemList)
                .read("\nChoose a parameter or external resource dependency.");
        if (choice instanceof ParameterEditingMenuItem) {
            ParameterEditingMenuItem item = (ParameterEditingMenuItem) choice;
            item.setParameterValue(textIO);
            clearTerminal(textIO);
        } else if (choice instanceof MultiValuedParameterEditingMenuItem) {
            MultiValuedParameterEditingMenuItem item = (MultiValuedParameterEditingMenuItem) choice;
            item.setParameterValue(textIO);
            clearTerminal(textIO);
        } else if (choice.getName().equals("<Component Name>")) {
            String name = textIO.newStringInputReader().read("Enter the new component name:");
            description.setName(name);
        } else if (choice instanceof ExternalResourceEditingMenuItem) {
            ExternalResourceEditingMenuItem item = (ExternalResourceEditingMenuItem) choice;
            Optional<ExternalResourceBinding> resourceBinding = Optional.empty();
            if (description.getDescriptorAsAnalysisEngineDescription().getResourceManagerConfiguration() != null) {
                resourceBinding = Stream.of(description.getDescriptorAsAnalysisEngineDescription().
                        getResourceManagerConfiguration().getExternalResourceBindings()).
                        filter(b -> b.getKey().equals(item.getDependency().getKey())).
                        findFirst();
            }
            try {
                String answer = "new";
                if (resourceBinding.isPresent())
                    answer = textIO.newStringInputReader().withInlinePossibleValues("edit", "new").read("Do you want to edit the existing resource or create a new one?");
                if (answer.equals("new")) {
                    new ExternalResourceDefinitionDialog(pipeline, description).createExternalResourceBinding(textIO, item.getDependency());
                } else {
                    String resourceName = resourceBinding.get().getResourceName();
                    Optional<ExternalResourceDescription> resource = Stream.of(description.getDescriptorAsAnalysisEngineDescription().
                            getResourceManagerConfiguration().getExternalResources()).
                            filter(res -> res.getName().equals(resourceName)).
                            findFirst();
                    if (resource.isPresent()) {
                        new ExternalResourceConfigurationDialog(description, resource.get()).enterInputLoop(textIO, path);
                    }
                }
            } catch (MenuItemExecutionException e) {
                log.error("External resource configuration failed: ", e);
            }
        } else if (choice instanceof ExternalResourceDependencyDefinitionMenuItem) {
            clearTerminal(textIO);
            String key = textIO.newStringInputReader().withMinLength(0).read("Specify the key of the resource dependency (empty line to abort):");
            if (key.length() > 0) {
                String description = textIO.newStringInputReader().withMaxLength(0).read("Specify the description of the resource dependency:");
                String interfaceName = textIO.newStringInputReader().read("Specify the interface name of the resource dependency:");
                boolean isOptional = textIO.newBooleanInputReader().read("Is this dependency optional?");

                ExternalResourceDependency dep = new ExternalResourceDependency_impl();
                dep.setInterfaceName(interfaceName);
                dep.setKey(key);
                dep.setOptional(isOptional);
                dep.setDescription(description);

                ExternalResourceDependency[] externalResourceDependencies = this.description.getDescriptorAsAnalysisEngineDescription().getExternalResourceDependencies();
                ExternalResourceDependency[] newDependencies = new ExternalResourceDependency[externalResourceDependencies != null ? externalResourceDependencies.length + 1 : 1];
                if (externalResourceDependencies != null)
                    System.arraycopy(externalResourceDependencies, 0, newDependencies, 0, externalResourceDependencies.length);
                newDependencies[newDependencies.length - 1] = dep;
                this.description.getDescriptorAsAnalysisEngineDescription().setExternalResourceDependencies(newDependencies);

                createMenuItems(this.description);

                if (textIO.newBooleanInputReader().read("Do you which to define an external resource for the new dependency?")) {
                    try {
                        new ExternalResourceDefinitionDialog(pipeline, this.description).createExternalResourceBinding(textIO, dep);
                    } catch (MenuItemExecutionException e) {
                        log.error("External resource configuration failed: ", e);
                    }
                }
            }
        }
        return choice;
    }
}
