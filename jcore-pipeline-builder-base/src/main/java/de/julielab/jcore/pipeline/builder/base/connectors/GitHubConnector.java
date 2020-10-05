package de.julielab.jcore.pipeline.builder.base.connectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ximpleware.ParseException;
import com.ximpleware.XPathParseException;
import de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants;
import de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants.GitHub;
import de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants.JcoreMeta;
import de.julielab.jcore.pipeline.builder.base.exceptions.GithubInformationException;
import de.julielab.jcore.pipeline.builder.base.main.GitHubRepository;
import de.julielab.jcore.pipeline.builder.base.main.MetaDescription;
import de.julielab.xml.JulieXMLTools;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.util.*;

public class GitHubConnector {

    private static Map<String, MetaDescription> componentMap = null;
    private static final Logger log = LoggerFactory.getLogger(GitHubConnector.class);

    /**
     * Builds a URL of the form "https://raw.githubusercontent.com/JULIELab/{@code module}/{@code version}/{@code name}/component.meta
     *
     * @param name e.g. "jcore-ace-reader"
     * @return
     */
    private static URL getComponentMetaURL(String name, GitHubRepository repository) {
        try {
            URIBuilder builder = new URIBuilder();
            builder.setScheme(GitHub.SCHEME);
            builder.setHost(GitHub.RAW);
            builder.setPath(new StringJoiner("/").add("").add(repository.getGitHubName()).add(repository.getName()).add(repository.getVersion()).add(name)
                    .add(JcoreMeta.FILE).toString());

            return builder.build().toURL();
        } catch (URISyntaxException | MalformedURLException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static URL getGitHubBranchListApiRequestURL(String user, String repositoryName) {
        URIBuilder builder = new URIBuilder();
        builder.setScheme(GitHub.SCHEME);
        builder.setHost(GitHub.API);
        builder.setPath(new StringJoiner("/")
                .add("")
                .add(GitHub.API_REPO)
                .add(user)
                .add(repositoryName)
                .add(GitHub.API_BRANCHES).toString());
        try {
            return builder.build().toURL();
        } catch (URISyntaxException | MalformedURLException e) {
            log.error("Could not build GitHub branch request URI with user {} and repository name {}", user, repositoryName, e);
        }
        return null;
    }

    public static List<RepositoryBranchInformation> getRepositoryBranches(GitHubRepository repository) {
        return getRepositoryBranches(repository.getGitHubName(), repository.getName());
    }

    public static List<RepositoryBranchInformation> getRepositoryBranches(String user, String repositoryName) {
        try {
            final ObjectMapper om = new ObjectMapper();
            return Arrays.asList(om.readValue(getGitHubBranchListApiRequestURL(user, repositoryName), RepositoryBranchInformation[].class));
        } catch (IOException e) {
            log.error("Exception while trying to read list of branches from {}", getGitHubBranchListApiRequestURL(user, repositoryName ), e);
        }
        return Collections.emptyList();
    }

    /**
     * Builds a URL of the form "https://raw.githubusercontent.com/JULIELab/{@code module}/{@code version}/{@code name}/pom.xml
     *
     * @param name    e.g. "jcore-ace-reader"
     * @param module  e.g. "jcore-base"
     * @param version e.g. "2.3.0-SNAPSHOT"
     * @return
     */
    private static URL getComponentPomURL(String user, String name, String module, String version) {
        try {
            URIBuilder builder = new URIBuilder();
            builder.setScheme(GitHub.SCHEME);
            builder.setHost(GitHub.RAW);
            builder.setPath(new StringJoiner("/")
                    .add("")
                    .add(user)
                    .add(module)
                    .add(version)
                    .add(name)
                    .add(PipelineBuilderConstants.Maven.POM).toString());

            return builder.build().toURL();
        } catch (URISyntaxException | MalformedURLException e) {
            log.error("Could not build GitHub component POM request for component name {}, repository {} and branch {}", name, module, version, e);
        }
        return null;
    }

    /**
     * Builds a URL of the form "https://api.github.com/repos/JULIELab/$repo$/contents"
     *
     * @return
     */
    private static URL getRepoContentsURL(GitHubRepository repository) {
        try {
            URIBuilder builder = new URIBuilder();
            builder.setScheme(GitHub.SCHEME);
            builder.setHost(GitHub.API);
            builder.setPath(new StringJoiner("/").add("").add(GitHub.API_REPO).add(repository.getGitHubName()).add(repository.getName())
                    .add(GitHub.API_CONTENT).toString());
            builder.addParameter(GitHub.API_VERSION_PARAMETER, repository.getVersion());
            String accessToken = System.getProperty("github.api.accesstoken");
            String username = System.getProperty("github.api.username");
            String passwd = System.getProperty("github.api.password");
            if (accessToken != null)
                builder.addParameter("access_token", accessToken);
            else if (username != null && passwd != null)
                builder.setUserInfo(username, passwd);
            else
                log.warn("Neither a GitHub OAuth token nor username and password were given. Thus, your requests " +
                        "to GitHub for retrieving the JCoRe component list and their meta descriptions are limited. " +
                        "If this is an issue, provide your access token or your user name and password for GitHub via " +
                        "the github.api.accesstoken, github.api.username and github.api.password Java system " +
                        "properties.");
            return builder.build().toURL();
        } catch (MalformedURLException | URISyntaxException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static URLConnection connectWithHeader(URL url) throws IOException, GithubInformationException {
        URLConnection connection = url.openConnection();
        connection.addRequestProperty("User-Agent", "JCoRe Pipeline Builder");
        try {
            Integer rateLimit = Integer.parseInt(connection.getHeaderField("X-RateLimit-Limit"));
            int rateLimitRemaining = Integer.parseInt(connection.getHeaderField("X-RateLimit-Remaining"));
            log.debug("GitHub API requests rate limit: {} of {} remaining", rateLimitRemaining, rateLimit);
            if (rateLimitRemaining == 0)
                throw new GithubInformationException("Cannot request JCoRe component meta data from GitHub due to " +
                        "the request rate limit imposed by GitHub. Your current rate limit is " + rateLimit + " and no " +
                        "requests remain within the hour. Provide your access token or your user name " +
                        "and password for GitHub via " +
                        "the github.api.accesstoken, github.api.username and github.api.password Java system " +
                        "properties to raise your rate limit. For more information, see " +
                        "https://developer.github.com/v3/#rate-limiting");
        } catch (NumberFormatException e) {
            // We use this method also for non-github API requests, thus just don't do anything when the fields
            // are not present
        }
        return connection;
    }

    /**
     * @return
     */
    private static MetaDescription checkForExposable(String component, GitHubRepository repository) throws IOException, GithubInformationException {
        InputStream metaFile = getMetaFileStream(component, repository);
        if (metaFile != null) {
            ObjectMapper mapper = new ObjectMapper();
            MetaDescription metaDescription;
            try {
                metaDescription = mapper.readValue(metaFile, MetaDescription.class);
            } catch (JsonProcessingException e) {
                log.error("JSON reading exception for the meta descriptor of component {} in repository {}, version {}",
                        component, repository.getName(), repository.getVersion());
                throw e;
            }
            if (metaDescription.isExposable()) {
                return metaDescription;
            } else {
                log.info(String.format(
                        "'%s' has a meta file, but is skipped due to being not exposed to the builder.", component));
            }
        }
        return null;
    }

    /**
     * Gets the content of a JCoRe component meta file
     *
     * @param name e.g. "jcore-ace-reader"
     * @return
     */
    public static InputStream getMetaFileStream(String name, GitHubRepository repository) throws GithubInformationException {
        URL meta = getComponentMetaURL(name, repository);

        try {
            return meta.openStream();
        } catch (IOException e) {
            log.info(String.format("URL was not found: %s", meta.toString()));
            return null;
        } catch (NullPointerException e) {
            log.error(String.format("URL could not be built for component '%s' in module '%s' for version '%s'.",
                    name, repository.getName(), repository.getVersion()), e);
            return null;
        }
    }

    /**
     * Gets the content of a JCoRe component pom.xml file
     *
     * @param name    e.g. "jcore-ace-reader"
     * @param module  e.g. "jcore-base"
     * @param version e.g. "2.3.0-SNAPSHOT"
     * @return
     */
    public static InputStream getPomFileStream(String user, String name, String module, String version) throws GithubInformationException {
        URL meta = getComponentPomURL(user, name, module, version);

        try {
            return meta.openStream();
        } catch (IOException e) {
            log.info(String.format("URL was not found: %s", meta.toString()));
            return null;
        } catch (NullPointerException e) {
            log.error(String.format("URL could not be built for component '%s' in module '%s' for version '%s'.",
                    name, module, version), e);
            return null;
        }
    }

    /**
     * Gets the content of a GitHub repository in JSON format
     *
     * @return
     */
    public static InputStream getGitHubContentStream(GitHubRepository repository) throws GithubInformationException {
        URL meta = getRepoContentsURL(repository);
        try {
            return connectWithHeader(meta).getInputStream();
        } catch (IOException | NullPointerException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static void listComponents(GitHubRepository repository, Boolean exposable) throws IOException, GithubInformationException {
        log.trace("Fetching JCoRe component list for module {}:{}. Only exposable parameters are fetched: {}", repository.getName(), repository.getVersion(), exposable);
        Map<String, MetaDescription> componentList = new HashMap<>();
        InputStream is = getGitHubContentStream(repository);
        JsonReader rdr = Json.createReader(is);
        for (JsonObject result : rdr.readArray().getValuesAs(JsonObject.class)) {
            if (result.getString(GitHub.API_TYPE).equals(GitHub.API_FOLDER)) {
                String component = result.getString(GitHub.API_PATH);
                log.trace("Fetching meta description of component {}", component);
                MetaDescription metaFile = checkForExposable(component, repository);
                if (exposable && (metaFile == null)) {
                    log.trace("Component {} is skipped because only exposable components are loaded and the component does not have a meta description file.", component);
                    continue;
                }
                try {
                    // The project directory name is not always the exact maven artifact ID, even if it should be that
                    // way. Here we retrieve the actual artifact ID.
                    component = JulieXMLTools.getXpathValue("/project/artifactId",
                            getPomFileStream(repository.getGitHubName(), component, repository.getName(), repository.getVersion()));
                    log.trace("The parsed artifact ID for the component is {}", component);
                } catch (XPathParseException | ParseException e) {
                    throw new IOException(e);
                }
                componentList.put(component, metaFile);
            }
        }
        componentMap = componentList;
    }

    /**
     * Lists all folders in a the repository $repo$ of branch %version% with their respective MetaFile InputStream.
     * %exposable% is checked against the flag 'exposable' in the 'component.meta' file of each folder (i.e. component).
     * If the component has no 'component.meta', it is not listed as well.
     *
     * @param exposable should it only include components that can be used as 'building blocks' for a pipeline
     * @return
     */
    public static Map<String, MetaDescription> getComponents(GitHubRepository repository, Boolean exposable) throws IOException, GithubInformationException {
        listComponents(repository, exposable);
        return componentMap;
    }

//    public static Map<String, JsonObject> getJcoreComponents() {
//        if (componentMap == null) {
//            listJcoreComponents("jcore-base", "master", true);
//        }
//        return componentMap;
//    }
}
