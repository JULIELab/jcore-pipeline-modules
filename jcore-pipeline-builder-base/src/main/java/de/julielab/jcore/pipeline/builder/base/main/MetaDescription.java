package de.julielab.jcore.pipeline.builder.base.main;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.util.StdConverter;
import de.julielab.jcore.pipeline.builder.base.exceptions.DescriptorLoadingException;
import de.julielab.jcore.pipeline.builder.base.exceptions.MetaDescriptionInteractionException;
import de.julielab.jcore.pipeline.builder.base.interfaces.IMetaDescription;
import de.julielab.jcore.pipeline.builder.base.utils.DescriptorUtils;
import de.julielab.utilities.aether.MavenArtifact;
import de.julielab.utilities.aether.MavenException;
import org.apache.uima.resource.ResourceSpecifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.function.Supplier;

import static de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants.JcoreMeta;

@JsonDeserialize(converter = MetaDescription.FromJsonSetup.class)
public class MetaDescription implements IMetaDescription, Serializable {
    /**
     * This version UID is manually maintained. Thus it is important to change it when the class actually becomes
     * incompatible with older version.
     */
    private static final long serialVersionUID = 2018_06_21_001L;
    private static Logger logger = LoggerFactory.getLogger(MetaDescription.class);
    private String description;
    private Map<String, Description> descriptionMap = new HashMap<>();
    private String group;
    private ComponentRepository module;
    private String base;
    @JsonProperty("name")
    private String componentName;
    private boolean exposable;
    @JsonProperty("base-project")
    private String baseProject;
    @JsonProperty("maven-artifact")
    private MavenArtifact artifact;
    private List<JcoreMeta.Category> categories;
    private Integer chosenDescriptor;
    @JsonProperty("descriptors")
    private List<Description> descriptorList = new ArrayList<>();

    private Boolean isPear = false;
    /**
     * Despite the fact that {@link #artifact} is loaded from the meta description JSON format, it must still be
     * 'initialized' through the {@link #initMavenArtifact()} method to obtain the actual JAR files from the local
     * or central Maven repository and retrieve a correct file reference. This variable is here to memorize if this
     * has already been done.
     */
    @JsonIgnore
    private boolean artifactInitialized = false;

    public MetaDescription() {
    }


    public Boolean isPear() {
        return isPear;
    }

    public void setPear(Boolean pear) {
        isPear = pear;
    }

    public String getBaseProject() {
        return baseProject;
    }

    public void setBaseProject(String baseProject) {
        this.baseProject = baseProject;
    }

    public String getDescription() throws DescriptorLoadingException {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * The name of the base component. May be null if this is a base component.
     *
     * @return
     */
    public String getBase() {
        return base;
    }

    public void setBase(String base) {
        this.base = base;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public boolean isExposable() {
        return exposable;
    }

    public void setExposable(boolean exposable) {
        this.exposable = exposable;
    }

    @JsonIgnore
    @Override
    public Description getChosenDescriptor() throws DescriptorLoadingException {
        getJCoReDescriptions();
        if (chosenDescriptor != null) {
            if (chosenDescriptor > descriptorList.size()) {
                chosenDescriptor = descriptorList.size() - 1;
            }
            return descriptorList.get(chosenDescriptor);
        }
        throw new IllegalStateException("No descriptor has been chosen.");
    }

    @Override
    public void setChosenDescriptor(String xmlName) throws DescriptorLoadingException {
        getJCoReDescriptions();
        if (!descriptionMap.containsKey(xmlName))
            throw new IllegalArgumentException("There is no descriptor with XML name " + xmlName);
        this.chosenDescriptor = descriptorList.indexOf(descriptionMap.get(xmlName));
    }

    private void loadDescriptorsFromMavenArtifact() throws DescriptorLoadingException {
        // For PEAR components there is nothing to do here. They can't be configured externally anyway.
        if (!isPear) {
            initMavenArtifact();
            try {
                for (Description description : descriptorList) {
                    ResourceSpecifier spec = DescriptorUtils.searchDescriptor(artifact.getFile(), description.getLocation());
                    description.setMetaDescription(this);
                    description.setDescriptor(spec);
                    descriptionMap.put(description.getLocation(), description);
                }
            } catch (IOException e) {
                throw new MetaDescriptionInteractionException(e);
            }
        }
        if (descriptionMap.size() >= 1)
            setChosenDescriptorAsIndex(0);
    }

    @JsonIgnore
    @Override
    public Integer getChosenDescriptorAsIndex() {
        return chosenDescriptor;
    }

    @Override
    public void setChosenDescriptorAsIndex(Integer descIndex) {
        this.chosenDescriptor = descIndex;
    }

    @JsonIgnore
    @Override
    public Description getJCoReDescription(String specifier) throws DescriptorLoadingException {
        getJCoReDescriptions();
        return this.descriptionMap.getOrDefault(specifier, null);
    }

    @JsonIgnore
    @Override
    public Collection<Description> getJCoReDescriptions() throws DescriptorLoadingException {
        if (descriptionMap.isEmpty()) {
            if (isPear) {
                // For PEAR components, we just take their descriptors as granted. We cannot modify them anyway.
                for (Description description : descriptorList) {
                    description.setMetaDescription(this);
                    descriptionMap.put(description.getLocation(), description);
                }
                if (descriptionMap.size() == 1)
                    setChosenDescriptorAsIndex(0);
            } else
                loadDescriptorsFromMavenArtifact();
        }
        return this.descriptionMap.values();
    }

    @Override
    public String getName() {
        return this.componentName;
    }

    public void setName(String name) { this.componentName = name; }

    @Override
    public List<JcoreMeta.Category> getCategories() {
        return this.categories;
    }

    @Override
    public String toString() {
        return "MetaDescription{" +
                "group='" + group + '\'' +
                ", module='" + module + '\'' +
                ", componentName='" + componentName + '\'' +
                '}';
    }

    public void setArtifact(MavenArtifact artifact) {
        this.artifact = artifact;
    }

    @JsonIgnore
    @Override
    public MavenArtifact getMavenArtifact() {
        if (!isPear && !artifactInitialized) {
            initMavenArtifact();
        }
        return artifact;
    }

    private void initMavenArtifact() {
        // Convenience to quickly get the meta descriptor for error reporting, if necessary
        Supplier<String> metaJsonSupplier = () -> {
            ObjectMapper mapper = new ObjectMapper();
            String metaJson = "<could not serialize>";
            try {
                metaJson = mapper.writeValueAsString(this);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            return metaJson;
        };

        try {
            List<MavenArtifact> mavenArtifacts = JcoreGithubInformationService.getInstance().downloadArtifacts(Collections.singletonList(artifact));
            MavenArtifact resolvedArtifact = mavenArtifacts.get(0);
            this.artifact.setFile(resolvedArtifact.getFile());
        } catch (MavenException e) {
            throw new MetaDescriptionInteractionException("Cannot initialize meta description because the " +
                    "Maven artifact could not be retrieved. The meta description is: " + metaJsonSupplier.get(), e);
        }
        setArtifact(artifact);
        artifactInitialized = true;
    }

    @JsonIgnore
    public ComponentRepository getModule() {
        return module;
    }

    public void setModule(ComponentRepository module) {
        this.module = module;
    }


    protected static final class FromJsonSetup extends StdConverter<MetaDescription, MetaDescription> {

        @Override
        public MetaDescription convert(MetaDescription value) {
            return value;
        }
    }

}
