package de.julielab.jcore.pipeline.builder.base.main;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import de.julielab.java.utilities.FileUtilities;
import de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants.JcoreMeta;
import de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants.Maven;
import de.julielab.jcore.pipeline.builder.base.connectors.GitHubConnector;
import de.julielab.jcore.pipeline.builder.base.exceptions.DescriptorLoadingException;
import de.julielab.jcore.pipeline.builder.base.exceptions.GithubInformationException;
import de.julielab.jcore.pipeline.builder.base.interfaces.IComponentMetaInformationService;
import de.julielab.utilities.aether.AetherUtilities;
import de.julielab.utilities.aether.MavenArtifact;
import de.julielab.utilities.aether.MavenException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.JsonException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class JcoreGithubInformationService implements IComponentMetaInformationService, Serializable {
    private static Logger logger = LoggerFactory.getLogger(JcoreGithubInformationService.class);
    private ComponentRepository[] repositories;
    private String mvnLocal;
    private Map<String, MetaDescription> metaInformation = new HashMap<>();
    private Set<MavenArtifact> mavenDependencies = new HashSet<>();
    private static JcoreGithubInformationService instance;

    public static JcoreGithubInformationService getInstance() {
        if (instance == null)
            instance = new JcoreGithubInformationService(Repositories.findRepositories());
        return instance;
    }


    private JcoreGithubInformationService(ComponentRepository[] repositories) {
        logger.debug("Initializing component repository service with the following repositories: {}", Arrays.toString(repositories));
        this.repositories = repositories;
        this.mvnLocal = Paths.get(System.getProperty("user.home"), Maven.LOCAL_REPO).toString();
    }


    public void completeReload() throws GithubInformationException, MavenException {
        this.metaInformation.clear();
        this.mavenDependencies.clear();
        loadComponentMetaInformation(true);
        loadAllArtifacts();
    }

    @Override
    public void saveMetaInformationToDisk(ComponentRepository repository) throws GithubInformationException {
        File metaFile;
        try {
            metaFile = Repositories.getJcoreMetaFile(repository);
            metaFile.getParentFile().mkdirs();
            if (!metaFile.createNewFile()) {
                metaFile.delete();
                metaFile.createNewFile();
            }
            ObjectMapper mapper = new ObjectMapper();
            mapper.setDefaultPropertyInclusion(JsonInclude.Include.NON_ABSENT);
            mapper.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);
            mapper.setDefaultPropertyInclusion(JsonInclude.Include.NON_EMPTY);
            mapper.addMixIn(Description.class, DescriptionRepositoryStorageMixin.class);
            // enable pretty printing
            mapper.enable(SerializationFeature.INDENT_OUTPUT);
            mapper.writeValue(FileUtilities.getWriterToFile(metaFile), getMetaInformation());
        } catch (IOException e) {
            throw new GithubInformationException(e);
        }
    }

    @Override
    public void loadMetaInformationFromDisk(ComponentRepository repository) throws GithubInformationException {
        logger.debug("Loading JCoRe component meta information from local file cache for repository {}.", repository);
        Map<String, MetaDescription> metaInf = new HashMap<>();
        String eMessage = null;
        InputStream infile = null;
        File metaFile = Repositories.getJcoreMetaFile(repository);
        logger.trace("Loading component meta description file {} for module {}:{}", metaFile, repository);
        try {
            infile = FileUtilities.getInputStreamFromFile(metaFile);
            ObjectMapper objectMapper = new ObjectMapper();
            List<MetaDescription> asList = objectMapper.readValue(
                    infile, new TypeReference<List<MetaDescription>>() {
                    });
            asList.forEach(md -> this.metaInformation.put(md.getName(), md));
            asList.forEach(md -> md.setModule(repository));
            if (logger.isTraceEnabled()) {
                asList.stream().map(MetaDescription::getName).forEach(name -> logger.trace("Loading meta description of {}", name));
            }
        } catch (JsonException | MismatchedInputException e) {
            try {
                infile.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            logger.info("JsonException while trying to read a stored meta information file. Reloading from GitHub.");
            logger.debug("The exception was", e);
            loadComponentMetaInformation(true, repository);
        } catch (IOException e) {
            throw new GithubInformationException(e);
        }
        if (eMessage != null) {
            logger.error(String.format("Could not load MetaInformation for %s-%s: %s", repository, eMessage));
        }
    }

    public void loadComponentMetaInformation(Boolean loadNew, ComponentRepository repository) throws GithubInformationException {
        logger.trace("Loading component meta data for repository {}:{}. The parameter 'loadNew' is set to {}", repository.getName(), repository.getVersion(), loadNew);
        try {
            if ((!loadNew || !repository.isUpdateable()) && Repositories.getJcoreMetaFile(repository).exists()) {
                this.loadMetaInformationFromDisk(repository);
            } else {
                logger.debug("Loading JCoRe component meta information from GitHub.");
                Map<String, MetaDescription> jc = GitHubConnector.getJcoreComponents((GitHubRepository) repository, true);
                for (String key : jc.keySet()) {
                    MetaDescription metaDescription = jc.get(key);
                    metaDescription.setModule(repository);
                    logger.trace("Loaded component {}", metaDescription);
                    this.metaInformation.put(key, metaDescription);
                }
                saveMetaInformationToDisk(repository);
            }
        } catch (IOException e) {
            throw new GithubInformationException(e);
        }
    }

    @Override
    public void loadComponentMetaInformation(Boolean loadNew) throws GithubInformationException {
        metaInformation.clear();
        for (Integer i = 0; i < this.repositories.length; i++) {
            loadComponentMetaInformation(loadNew, repositories[i]);
        }
    }


    @Override
    public List<MavenArtifact> downloadArtifacts(List<MavenArtifact> artifactIds) throws MavenException {
        logger.debug("Retrieving the Maven artifacts for {} components", artifactIds.size());
        List<MavenArtifact> resolvedArtifacts = new ArrayList<>();
        for (MavenArtifact artifactId : artifactIds) {
            if (this.mavenDependencies.contains(artifactId)) {
                continue;
            }
            try {
                MavenArtifact resolvedArtifact = AetherUtilities.getArtifactByAether(artifactId, new File(this.mvnLocal));
                resolvedArtifacts.add(resolvedArtifact);
            } catch (MavenException e) {
                resolvedArtifacts.add(null);
                throw e;
            }
        }
        return resolvedArtifacts;
    }

    @Override
    public Set<MavenArtifact> loadAllArtifacts() throws GithubInformationException, MavenException {
        List<MavenArtifact> dlList = new ArrayList<>();
        if (this.mavenDependencies.isEmpty()) {
            for (MetaDescription jobj : this.getMetaInformation()) {
                MavenArtifact mvnArtifact = jobj.getMavenArtifact();
                dlList.add(mvnArtifact);
            }
            this.downloadArtifacts(dlList);
        }
        return this.mavenDependencies;
    }

    @Override
    public Set<MavenArtifact> getArtifacts() throws GithubInformationException {
        return this.mavenDependencies;
    }

    //TODO right now the 'contents' of JsonObject are not known.
    //TODO to parse the information out later, one need to specify which var has only a string as value
    //TODO and which has for instance an array (e.g. potentially the value of 'descriptor-location')

    @Override
    public Collection<MetaDescription> getMetaInformation() throws GithubInformationException {
        return this.getMetaInformation(false);
    }

    @Override
    public Collection<MetaDescription> getMetaInformationWithArtifacts(Boolean loadNew) throws GithubInformationException, MavenException {
        // Load the meta information so that we know which artifacts to get. Then get the artifacts.
        // Then filter for the artifacts we got and return the result.
        getMetaInformation(loadNew);
        loadAllArtifacts();
        return getMetaInformation(false).stream().
                filter(md ->
                        mavenDependencies.contains(md.getMavenArtifact())
                ).
                collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public MetaDescription getMetaInformation(String componentName) throws GithubInformationException {
        return this.getMetaInformation(componentName, false);
    }

    @Override
    public Collection<MetaDescription> getMetaInformation(Boolean loadNew) throws GithubInformationException {
        if (this.metaInformation.isEmpty() || loadNew) {
            this.loadComponentMetaInformation(loadNew);
        }
        return this.metaInformation.values();
    }

    @Override
    public MetaDescription getMetaInformation(String componentName, Boolean loadNew) throws GithubInformationException {
        if (this.metaInformation.isEmpty() || loadNew) {
            this.loadComponentMetaInformation(loadNew);
        }
        return this.metaInformation.getOrDefault(componentName, null);
    }

    /**
     * @param componentName
     * @return
     */
    public String getName(String componentName) throws GithubInformationException {
        return this.getMetaInformation(componentName).getName();
    }

    /**
     * @param componentName
     * @return
     */
    public String[] getDescriptors(String componentName) throws GithubInformationException, DescriptorLoadingException {
        MetaDescription jobj = this.getMetaInformation(componentName);
        return jobj.getJCoReDescriptions().stream().map(Description::getLocation).toArray(String[]::new);
    }

    /**
     * @param componentName
     * @return
     */
    public List<String> getCategory(String componentName) throws GithubInformationException {
        return this.getMetaInformation(componentName).getCategories().stream().map(JcoreMeta.Category::name).collect(Collectors.toList());
    }

    //ToDo: it should be possible to specify multiple groups in the meta file and return a String Array
    //ToDo: (like getDescriptors)

    /**
     * @param componentName is a key of this.mavenDependencies
     * @return
     */
    public String getGroups(String componentName) throws GithubInformationException {
        return this.getMetaInformation(componentName).getGroup();
    }

}
