package de.julielab.jcore.pipeline.builder.base.main;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.julielab.java.utilities.UriUtilities;
import de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants.Descriptor;
import de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants.JcoreMeta;
import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.TypeOrFeature;
import org.apache.uima.collection.CasConsumerDescription;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.ConfigurationParameterFactory;
import org.apache.uima.resource.ResourceCreationSpecifier;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.Capability;
import org.apache.uima.resource.metadata.ConfigurationParameter;
import org.apache.uima.resource.metadata.MetaDataObject;
import org.apache.uima.resource.metadata.ResourceMetaData;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.XMLInputSource;
import org.apache.uima.util.XMLParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;

public class Description implements Serializable, Cloneable {

    /**
     * This version UID is manually maintained. Thus it is important to change it when the class actually becomes
     * incompatible with older version.
     */
    private static final long serialVersionUID = 2018_06_19_001L;
    private static Logger logger = LoggerFactory.getLogger(Description.class);
    private URI uri;
    private String location;
    /**
     * The category as found in the component meta description.
     */
    private JcoreMeta.Category category;
    private String xmlName;
    private MetaDataObject specifier;

    public void setUri(URI uri) {
        this.uri = uri;
    }

    /**
     * The descriptor category as read from the actual descriptor.
     */

    private String descriptorType;
    private Map<String, ArrayList<String>> capabilities = new HashMap<String, ArrayList<String>>() {{
        put(Descriptor.CAPABILITIES_IN, new ArrayList<>());
        put(Descriptor.CAPABILITIES_OUT, new ArrayList<>());
    }};
    private Boolean initCapabilities = false;
    private Map<String, ConfigurationParameter> configurationParameter = null;
    private MetaDescription metaDescription;

    public Description(URL sourceUrl) throws URISyntaxException, IOException, InvalidXMLException {
        if (sourceUrl != null) {
            this.uri = sourceUrl.toURI();
            parseDescXml(UriUtilities.getInputStreamFromUri(sourceUrl.toURI()), xmlName);
        }
    }

    public URI getUri() {
        return uri;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public JcoreMeta.Category getCategory() {
        return category;
    }

    public void setCategory(JcoreMeta.Category category) {
        this.category = category;
    }

    @JsonIgnore
    public String getDescriptorType() {
        return descriptorType;
    }

    private void parseDescXml(InputStream desc, String xmlName) throws InvalidXMLException {
        XMLParser parser = UIMAFramework.getXMLParser();
        ResourceSpecifier spec;
        try {
            spec = parser.parseResourceSpecifier(new XMLInputSource(desc, null));
        } catch (InvalidXMLException e) {
            logger.debug("XML {} could not be parsed as a UIMA component and is skipped.", xmlName);
            throw e;
        }
        if (spec != null) {
            specifier = spec;
            if (spec instanceof CollectionReaderDescription) {
                descriptorType = JcoreMeta.CATEGORY_READER;
            } else if (spec instanceof AnalysisEngineDescription) {
                descriptorType = JcoreMeta.CATEGORY_AE + "|" + JcoreMeta.CATEGORY_MULTIPLIER;
            } else if (spec instanceof CasConsumerDescription) {
                descriptorType = JcoreMeta.CATEGORY_CONSUMER;
            }
        }
    }

    private ResourceMetaData getMetaData() {
        return specifier != null && specifier instanceof ResourceCreationSpecifier ? ((ResourceCreationSpecifier) specifier).getMetaData() : null;
    }

    @JsonIgnore
    public String getXmlName() {
        return this.xmlName;
    }

    public void setXmlName(String xmlName) {
        this.xmlName = xmlName;
    }

    public void addCapability(String type, String value) {
        if (this.capabilities.containsKey(type)) {
            this.capabilities.get(type).add(value);
        }
    }

    public void setCapabilities(String type, List<String> values) {

    }

    @JsonIgnore
    public List<String> getCapabilities(String type) {
        if (!initCapabilities) {
            Capability[] capsArray = (Capability[]) getMetaData().getAttributeValue(Descriptor.CAPABILITIES);
            for (int i = 0; i < capsArray[0].getInputs().length; i++) {
                TypeOrFeature tof = capsArray[0].getInputs()[i];
                addCapability(Descriptor.CAPABILITIES_IN, tof.getName());
            }
            for (int i = 0; i < capsArray[0].getOutputs().length; i++) {
                TypeOrFeature tof = capsArray[0].getOutputs()[i];
                addCapability(Descriptor.CAPABILITIES_OUT, tof.getName());
            }
            initCapabilities = true;
        }
        return this.capabilities.getOrDefault(type, null);
    }

    @JsonIgnore
    public String getName() {
        return getMetaData() != null ? getMetaData().getName() : getMetaDescription().getName();
    }

    public void setName(String name) {
        if (getMetaData() != null) getMetaData().setName(name);
        else getMetaDescription().setName(name);
    }

    @JsonIgnore
    public Map<String, ConfigurationParameter> getConfigurationParameters() {
        if (this.configurationParameter == null) {
            this.configurationParameter = new HashMap<>();
            for (ConfigurationParameter cp :
                    getMetaData().getConfigurationParameterDeclarations().getConfigurationParameters()) {
                this.configurationParameter.put(cp.getName(), cp);
            }
        }
        return this.configurationParameter;
    }

    @JsonIgnore
    public Object getConfigurationParameterValue(String parameter) {
        if (!(this.specifier instanceof ResourceCreationSpecifier))
            throw new IllegalArgumentException("The descriptor " + getName() + " is of class " + this.specifier.getClass().getCanonicalName() + ". A " + ResourceSpecifier.class.getCanonicalName() + " is required.");
        if (this.getConfigurationParameters().containsKey(parameter)) {
            return ConfigurationParameterFactory.getParameterSettings((ResourceSpecifier) this.specifier).getOrDefault(parameter, null);
        }
        return null;
    }

    public void setConfigurationParameterValue(String key, Object value) {
        if (!(this.specifier instanceof ResourceCreationSpecifier))
            throw new IllegalArgumentException("The descriptor " + getName() + " is of class " + this.specifier.getClass().getCanonicalName() + ". A " + ResourceSpecifier.class.getCanonicalName() + " is required.");
        ConfigurationParameterFactory.setParameter((ResourceSpecifier) this.specifier, key, value);
    }

    @JsonIgnore
    public MetaDataObject getDescriptor() {
        return specifier;
    }

    public void setDescriptor(MetaDataObject descriptor) {
        this.specifier = descriptor;
    }

    @JsonIgnore
    public CollectionReaderDescription getDescriptorAsCollectionReaderDescription() {
        return (CollectionReaderDescription) specifier;
    }

    @JsonIgnore
    public AnalysisEngineDescription getDescriptorAsAnalysisEngineDescription() {
        return (AnalysisEngineDescription) specifier;
    }

    /**
     * @param descStream
     * @throws InvalidXMLException
     * @deprecated The resource specifier is directly set from {@link MetaDescription#loadDescriptorsFromMavenArtifact()} method
     */
    public void loadDescriptor(InputStream descStream) throws InvalidXMLException {
        parseDescXml(descStream, getXmlName() != null ? getXmlName() : "<XML descriptor location not set>");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Description that = (Description) o;
        return Objects.equals(uri, that.uri) &&
                Objects.equals(location, that.location) &&
                category == that.category &&
                Objects.equals(specifier, that.specifier) &&
                Objects.equals(descriptorType, that.descriptorType) &&
                Objects.equals(capabilities, that.capabilities) &&
                Objects.equals(configurationParameter, that.configurationParameter) &&
                Objects.equals(metaDescription, that.metaDescription) &&
                Objects.equals(getName(), that.getName());
    }

    @Override
    public int hashCode() {

        return Objects.hash(uri, location, category, specifier, descriptorType, capabilities, configurationParameter, metaDescription, getName());
    }

    @JsonIgnore
    public MetaDescription getMetaDescription() {
        return metaDescription;
    }

    public void setMetaDescription(MetaDescription metaDescription) {
        this.metaDescription = metaDescription;
    }

    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            logger.debug("Error occurred when serializing description for toString(), only returning the name: ", e);
            return getName();
        }
    }

    @Override
    public Description clone() throws CloneNotSupportedException {
        Description clone = (Description) super.clone();
        if (specifier != null)
            clone.specifier = (MetaDataObject) specifier.clone();
        clone.configurationParameter = null;
        return clone;
    }
}
