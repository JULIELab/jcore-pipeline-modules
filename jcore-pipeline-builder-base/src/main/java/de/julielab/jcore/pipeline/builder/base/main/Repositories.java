package de.julielab.jcore.pipeline.builder.base.main;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import de.julielab.java.utilities.FileUtilities;
import de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants;
import de.julielab.jcore.pipeline.builder.base.exceptions.GithubInformationException;
import java_cup.version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants.JcoreMeta.LOCAL_STORAGE;
import static de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants.JcoreMeta.REPOSITORIES;

/**
 * Manages the local repository meta files in JSON format. Those are stored in the home directory of the user.
 */
public class Repositories {

    /**
     * These are the GitHub locations of the JCoRe repositories, JCoRe Base and JCoRe projects. They currently serve
     * as default repositories to choose from for the pipeline builders.
     */
    public static final List<ComponentRepository> JCORE_REPOSITORIES = Arrays.asList(new GitHubRepository("jcore-base", null, "JULIELab"),
            new GitHubRepository("jcore-projects", null, "JULIELab")
    );
    private final static Logger log = LoggerFactory.getLogger(Repositories.class);

    private static List<ComponentRepository> usedRepositories = new ArrayList<>();

    static {
        File localdir = new File(LOCAL_STORAGE);
        if (!localdir.exists())
            localdir.mkdirs();
    }

    private Repositories() {
    }

    /**
     * Looks in the local repository JSON meta information file for currently used repositories. Found repositories
     * are set to this class as the used repositories from which components will be offered.
     */
    public static List<ComponentRepository> loadLocalRepositories() {
        File repositoriesFile = new File(LOCAL_STORAGE + File.separator + REPOSITORIES);
        ObjectMapper om = new ObjectMapper();
        try {
            if (repositoriesFile.exists())
                usedRepositories = Arrays.asList(om.readValue(repositoriesFile, ComponentRepository[].class));
        } catch (IOException e) {
            log.error("Could not load available repositories: ", e);
        }
        return usedRepositories;
    }

    /**
     * Returns the repositories that are currently in use, i.e. for which the JSON meta data is stored
     * in the home directory.
     *
     * @return A stream of repositories currently used by the pipeline builder.
     */
    public static Stream<ComponentRepository> getRepositories() {
        return getRepositories(null);
    }

    /**
     * Returns the repositories that are currently in use, i.e. for which the JSON meta data is stored
     * in the home directory. The optional <tt>filter</tt> predicate may be used to only get specific repositories back.
     *
     * @param filter A filter to restrict the returned repositories.
     * @return A - possibly filtered - stream of repositories currently used by the pipeline builder.
     */
    public static Stream<ComponentRepository> getRepositories(Predicate<ComponentRepository> filter) {
        if (filter == null)
            return usedRepositories.stream();
        return usedRepositories.stream().filter(filter);
    }

    /**
     * Loads existing repositories from the JSON file in the {@link PipelineBuilderConstants.JcoreMeta#LOCAL_STORAGE}, if it exists, and adds the given repositories. If the file does not exist, it is created and initialized with the given repositories. Note that this can be used to circumvent the addition of the JCoRe repositories when this method is called before the first call to {@link #loadLocalRepositories()}.
     *
     * @param repositories
     * @return
     * @throws IOException
     */
    public static void addRepositories(ComponentRepository... repositories) throws IOException {
        File repositoriesFile = new File(LOCAL_STORAGE + File.separator + REPOSITORIES);
        ObjectMapper om = new ObjectMapper();
        Stream.of(repositories).forEach(usedRepositories::add);
        om.writeValue(new FileOutputStream(repositoriesFile), usedRepositories);
    }

    public static File getMetaFile(ComponentRepository repository) {
        return getMetaFile(repository.getName(), repository.getVersion());
    }

    public static File getMetaFile(String repositoryName, String repositoryVersion) {
        return new File(new StringJoiner(File.separator)
                .add(LOCAL_STORAGE)
                .add(repositoryName)
                .add(repositoryVersion)
                .add("componentlist.json")
                .toString());
    }

    public static void saveMetaInformationToDisk(ComponentRepository repository) throws GithubInformationException {
        File metaFile;
        try {
            metaFile = Repositories.getMetaFile(repository);
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
            mapper.writeValue(FileUtilities.getWriterToFile(metaFile), JcoreGithubInformationService.getInstance().getMetaInformation());
        } catch (IOException e) {
            throw new GithubInformationException(e);
        }
    }

    public boolean hasComponentListMetaFile(ComponentRepository repository) {
        return getMetaFile(repository).exists();
    }

    public static void deleteComponentList(String repositoryName, String version) {
        final File metaFile = getMetaFile(repositoryName, version);
        if (metaFile.exists()) {
            Stream.of(metaFile.getParentFile().listFiles()).forEach(File::delete);
            metaFile.getParentFile().delete();
        }
    }
}
