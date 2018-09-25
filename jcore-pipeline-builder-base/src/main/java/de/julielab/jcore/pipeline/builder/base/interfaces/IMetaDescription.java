package de.julielab.jcore.pipeline.builder.base.interfaces;

import de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants;
import de.julielab.jcore.pipeline.builder.base.exceptions.DescriptorLoadingException;
import de.julielab.jcore.pipeline.builder.base.main.Description;
import de.julielab.jcore.pipeline.builder.base.main.MavenArtifact;

import java.util.Collection;
import java.util.List;

public interface IMetaDescription {

    Description getChosenDescriptor() throws DescriptorLoadingException;

    void setChosenDescriptor(String descriptor) throws DescriptorLoadingException;

    Integer getChosenDescriptorAsIndex();

    /**
     * Sets the chosen Descriptor as index reference to the Descriptor Array List
     * @param descIndex
     */
    void setChosenDescriptorAsIndex(Integer descIndex);

    /**
     * Returns a specific descriptor object.
     *
     * @param specifier
     * @return
     */
    Description getJCoReDescription(String specifier) throws DescriptorLoadingException;

    /**
     * Returns a collection of all the descriptor names for this component.
     *
     * The names can be used to access a specific descriptor object via the {@code getJCoReDescription} method.
     *
     * @return
     */
    Collection<Description> getJCoReDescriptions() throws DescriptorLoadingException;

    /**
     * Returns the visible name of this component.
     *
     * @return
     */
    String getName();

    List<PipelineBuilderConstants.JcoreMeta.Category> getCategories();

    MavenArtifact getMavenArtifact() throws DescriptorLoadingException;
}
