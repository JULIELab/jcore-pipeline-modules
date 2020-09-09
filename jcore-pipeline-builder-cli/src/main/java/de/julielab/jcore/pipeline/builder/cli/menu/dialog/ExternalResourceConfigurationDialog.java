package de.julielab.jcore.pipeline.builder.cli.menu.dialog;

import de.julielab.jcore.pipeline.builder.base.main.Description;
import de.julielab.jcore.pipeline.builder.cli.menu.BackMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.IMenuItem;
import de.julielab.jcore.pipeline.builder.cli.menu.ParameterEditingMenuItem;
import de.julielab.jcore.pipeline.builder.cli.util.StatusPrinter;
import org.apache.uima.resource.ConfigurableDataResourceSpecifier;
import org.apache.uima.resource.ExternalResourceDescription;
import org.apache.uima.resource.FileResourceSpecifier;
import org.apache.uima.resource.metadata.ExternalResourceBinding;
import org.beryx.textio.TextIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class ExternalResourceConfigurationDialog implements ILoopableDialog {
    private Description description;
    private ExternalResourceDescription resourceDescription;

    public ExternalResourceConfigurationDialog(Description description, ExternalResourceDescription resourceDescription) {
        this.description = description;
        this.resourceDescription = resourceDescription;
    }

    public IMenuItem executeMenuItem(TextIO textIO, Deque<String> path) {
        StatusPrinter.printComponentStatus(description, textIO);
        List<IMenuItem> itemList = new ArrayList<>();
        IMenuItem resourceUrlItem = new IMenuItem() {
            @Override
            public String getName() {
                return "Resource Url";
            }

            @Override
            public String toString() {
                return getName();
            }
        };
        IMenuItem resourceNameItem = new IMenuItem() {
            @Override
            public String getName() {
                return "Resource Name";
            }

            @Override
            public String toString() {
                return getName();
            }
        };
        IMenuItem resourceDescItem = new IMenuItem() {
            @Override
            public String getName() {
                return "Resource Description";
            }

            @Override
            public String toString() {
                return getName();
            }
        };
        itemList.add(resourceUrlItem);
        itemList.add(resourceNameItem);
        itemList.add(resourceDescItem);
        if (resourceDescription.getResourceSpecifier() instanceof FileResourceSpecifier) {
            IMenuItem response = textIO.<IMenuItem>newGenericInputReader(null).withNumberedPossibleValues(itemList).
                    withDefaultValue(BackMenuItem.get()).
                    read("Select the parameter you want to change:");
            if (response.equals(resourceUrlItem)) {
                FileResourceSpecifier spec = (FileResourceSpecifier) resourceDescription.getResourceSpecifier();
                String url = textIO.newStringInputReader().read("Enter the new file URL:");
                spec.setFileUrl(url);
            } else if (response.equals(resourceNameItem)) {
                renameExternalResource(textIO);
            } else if (response.equals(resourceDescItem)) {
                String desc = textIO.newStringInputReader().read("Enter the new resource description:");
                resourceDescription.setDescription(desc);
            }
            return BackMenuItem.get();
        } else if (resourceDescription.getResourceSpecifier() instanceof ConfigurableDataResourceSpecifier) {
            ConfigurableDataResourceSpecifier spec = (ConfigurableDataResourceSpecifier) resourceDescription.getResourceSpecifier();
            // Create the items for fixed information about resources: Their resource URL, their name and their description
            // Now add items for the parameters specific to this resource.
            Stream.of(spec.getMetaData().getConfigurationParameterDeclarations().
                    getConfigurationParameters()).
                    map(p -> new ParameterEditingMenuItem(resourceDescription.getResourceSpecifier(), p)).
                    forEach(itemList::add);
            itemList.add(BackMenuItem.get());
            IMenuItem response = textIO.<IMenuItem>newGenericInputReader(null).withNumberedPossibleValues(itemList)
                    .withDefaultValue(BackMenuItem.get())
                    .read("Select the parameter you want to change:");
            if (response.equals(resourceUrlItem)) {
                String url = textIO.newStringInputReader().withMinLength(0).read("Enter the new resource URL:");
                if (url.length() > 0)
                    spec.setUrl(url);
            } else if (response.equals(resourceNameItem)) {
                renameExternalResource(textIO);
            } else if (response.equals(resourceDescItem)) {
                String desc = textIO.newStringInputReader().withMinLength(0).read("Enter the new resource description:");
                if (desc.length() > 0)
                    resourceDescription.setDescription(desc);
            } else if (response instanceof ParameterEditingMenuItem) {
                ParameterEditingMenuItem item = (ParameterEditingMenuItem) response;
                item.setParameterValue(textIO);
            } else return response;
            clearTerminal(textIO);
            return response;
        }
        return BackMenuItem.get();
    }

    private void renameExternalResource(TextIO textIO) {
        String oldname = resourceDescription.getName();
        String name = textIO.newStringInputReader().read("Enter the new resource name:");
        resourceDescription.setName(name);
        Optional<ExternalResourceBinding> any = Stream.of(description.getDescriptorAsAnalysisEngineDescription().getResourceManagerConfiguration().getExternalResourceBindings()).filter(binding -> binding.getResourceName().equals(oldname)).findAny();
        // There might not (yet) be a binding
        if (any.isPresent())
            any.get().setResourceName(name);
    }

    @Override
    public String getName() {
        return "Configure External Resource";
    }

    @Override
    public String toString() {
        return getName();
    }


}
