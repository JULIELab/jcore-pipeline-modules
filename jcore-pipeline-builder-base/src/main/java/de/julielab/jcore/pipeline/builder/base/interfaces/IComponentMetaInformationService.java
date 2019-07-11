package de.julielab.jcore.pipeline.builder.base.interfaces;

import de.julielab.jcore.pipeline.builder.base.exceptions.DescriptorLoadingException;
import de.julielab.jcore.pipeline.builder.base.exceptions.GithubInformationException;
import de.julielab.jcore.pipeline.builder.base.main.ComponentRepository;
import de.julielab.jcore.pipeline.builder.base.main.MetaDescription;
import de.julielab.utilities.aether.MavenArtifact;
import de.julielab.utilities.aether.MavenException;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface IComponentMetaInformationService {

    /**
     *
     */
    //void saveMetaInformationToDisk(ComponentRepository repository) throws GithubInformationException;

    /**
     *
     */
    void loadMetaInformationFromDisk(ComponentRepository repository) throws IOException, GithubInformationException;

    /**
     * Builds a list of JsonObjects with their respective meta information.
     *
     */
    void loadComponentMetaInformation(Boolean loadNew) throws GithubInformationException;

    /**
     * Builds a map of "artifact name" - "artifact file" representations
     *
     * @param artifactIds a list of all the artifacts (their Maven ID) that should be downloaded.
     */
    List<MavenArtifact> downloadArtifacts(List<MavenArtifact> artifactIds) throws GithubInformationException, MavenException;

    /**
     *
     * @return
     */
    Set<MavenArtifact> loadAllArtifacts() throws GithubInformationException, DescriptorLoadingException, MavenException;

    Set<MavenArtifact> getArtifacts() throws GithubInformationException;

    Collection<MetaDescription> getMetaInformation(ComponentRepository repository) throws GithubInformationException;

    /**
     * Lists all exposed components of the module this ComponentMetaInformation object belongs to/
     * was created from.
     *
     * @return
     */
    Collection<MetaDescription> getMetaInformation() throws GithubInformationException;

    /**
     * Lists all exposed components with their Maven artifacts already set. Meta descriptions whose artifacts
     * cannot be found are filtered out.
     * @return
     * @throws GithubInformationException
     */
    Collection<MetaDescription> getMetaInformationWithArtifacts(Boolean loadNew) throws GithubInformationException, DescriptorLoadingException, MavenException;

    /**
     * Lists all exposed components of the module this ComponentMetaInformation object belongs to/
     * was created from.
     *
     * @param loadNew
     * @return
     */

    Collection<MetaDescription> getMetaInformation(Boolean loadNew) throws GithubInformationException;

    /**
     * Gets the meta information of a specific component.
     *
     * @param componentName
     * @return
     */
    MetaDescription getMetaInformation(String componentName) throws GithubInformationException;

    /**
     * Gets the meta information of a specific component.
     *
     * @param componentName
     * @param loadNew
     * @return
     */
    MetaDescription getMetaInformation(String componentName, Boolean loadNew) throws GithubInformationException;

    String[] getDescriptors(String componentName) throws GithubInformationException, DescriptorLoadingException;

    String getName(String componentName) throws GithubInformationException;

    List<String> getCategory(String componentName) throws GithubInformationException;

    String getGroups(String componentName) throws GithubInformationException;

    void completeReload() throws GithubInformationException, DescriptorLoadingException, MavenException;
}
