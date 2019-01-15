package de.julielab.jcore.pipeline.builder.base.main;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cglib.core.Local;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants.JcoreMeta.*;

public class Repositories {

    public static final ComponentRepository[] JCORE_REPOSITORIES = new ComponentRepository[]{new GitHubRepository("jcore-base", "v2.4", "JULIELab"),
            new GitHubRepository("jcore-projects", "2.3.0-SNAPSHOT", "JULIELab")
    };
    private final static Logger log = LoggerFactory.getLogger(Repositories.class);

    static {
        File localdir = new File(LOCAL_STORAGE);
        if (!localdir.exists())
            localdir.mkdirs();
    }

    private Repositories() {
    }

    /**
     * Looks for the file of available UIMA component repositories in the {@link PipelineBuilderConstants.JcoreMeta#LOCAL_STORAGE} directory. If this file does not yet exist, it is created and filled with the {@link #JCORE_REPOSITORIES}. Those repositories are then returned.
     * If other repositories should be used than JCoRe, call the {@link #addRepositories(ComponentRepository...)} method first.
     *
     * @return The available repositories.
     * @throws IOException
     */
    public static ComponentRepository[] findRepositories() {
        File repositoriesFile = new File(LOCAL_STORAGE + File.separator + REPOSITORIES);
        ObjectMapper om = new ObjectMapper();
        try {
            if (repositoriesFile.exists()) {
                return om.readValue(repositoriesFile, ComponentRepository[].class);
            } else {
                om.writeValue(new FileOutputStream(repositoriesFile), JCORE_REPOSITORIES);
                return JCORE_REPOSITORIES;
            }
        } catch (IOException e) {
            log.error("Could not load available repositories: ", e);
        }
        return new ComponentRepository[0];
    }

    /**
     * Loads existing repositories from the JSON file in the {@link PipelineBuilderConstants.JcoreMeta#LOCAL_STORAGE}, if it exists, and adds the given repositories. If the file does not exist, it is created and initialized with the given repositories. Note that this can be used to circumvent the addition of the JCoRe repositories when this method is called before the first call to {@link #findRepositories()}.
     *
     * @param repositories
     * @return
     * @throws IOException
     */
    public static ComponentRepository[] addRepositories(ComponentRepository... repositories) throws IOException {
        File repositoriesFile = new File(LOCAL_STORAGE + File.separator + REPOSITORIES);
        ObjectMapper om = new ObjectMapper();
        if (repositoriesFile.exists()) {
            List<ComponentRepository> repositoryList = Stream.concat(Stream.of(om.readValue(repositoriesFile, ComponentRepository[].class)),
                    Stream.of(repositories)).collect(Collectors.toList());
            om.writeValue(new FileOutputStream(repositoriesFile), repositoryList);
            return repositoryList.toArray(new ComponentRepository[repositoryList.size()]);
        } else {
            om.writeValue(new FileOutputStream(repositoriesFile), repositories);
            return repositories;
        }
    }

    public static File getJcoreMetaFile(ComponentRepository repository) {
        return new File(new StringJoiner(File.separator)
                .add(LOCAL_STORAGE)
                .add(repository.getName())
                .add(repository.getVersion())
                .add("componentlist.json")
                .toString());
    }
}
