package de.julielab.jcore.pipeline.builder.base.connectors;

import de.julielab.jcore.pipeline.builder.base.configurations.PipelineBuilderConstants.Maven;
import de.julielab.jcore.pipeline.builder.base.exceptions.IORuntimeException;
import de.julielab.jcore.pipeline.builder.base.exceptions.MavenException;
import de.julielab.jcore.pipeline.builder.base.main.MavenArtifact;
import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.apache.maven.settings.*;
import org.apache.maven.settings.building.*;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.collection.CollectResult;
import org.eclipse.aether.collection.DependencyCollectionException;
import org.eclipse.aether.connector.basic.BasicRepositoryConnectorFactory;
import org.eclipse.aether.graph.DefaultDependencyNode;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.graph.DependencyNode;
import org.eclipse.aether.impl.DefaultServiceLocator;
import org.eclipse.aether.internal.impl.DefaultRemoteRepositoryManager;
import org.eclipse.aether.repository.*;
import org.eclipse.aether.repository.RepositoryPolicy;
import org.eclipse.aether.resolution.*;
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory;
import org.eclipse.aether.spi.connector.transport.TransporterFactory;
import org.eclipse.aether.transport.file.FileTransporterFactory;
import org.eclipse.aether.transport.http.HttpTransporterFactory;
import org.eclipse.aether.util.repository.AuthenticationBuilder;
import org.eclipse.aether.util.repository.DefaultMirrorSelector;
import org.eclipse.aether.version.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Taken and adapted from:
 * https://stackoverflow.com/questions/48537735/download-artifact-from-maven-repository-in-java-program
 */

public class MavenConnector {
    private final static Logger log = LoggerFactory.getLogger(MavenConnector.class);

    private static final RemoteRepository central = new RemoteRepository.Builder("central", "default", "https://oss.sonatype.org/content/repositories/public/").setSnapshotPolicy(new RepositoryPolicy(true, RepositoryPolicy.UPDATE_POLICY_ALWAYS, RepositoryPolicy.CHECKSUM_POLICY_WARN)).build();

    private static RepositorySystem newRepositorySystem() {
        DefaultServiceLocator locator = MavenRepositorySystemUtils.newServiceLocator();
        locator.addService(RepositoryConnectorFactory.class, BasicRepositoryConnectorFactory.class);
        locator.addService(TransporterFactory.class, FileTransporterFactory.class);
        locator.addService(TransporterFactory.class, HttpTransporterFactory.class);
        return locator.getService(RepositorySystem.class);

    }

    private static RepositorySystemSession newSession(RepositorySystem system, File localRepository) throws SettingsBuildingException {
        DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();
        // The following line switches off the creation of the non-timestamp snapshot artifact file
        session.setConfigProperty("aether.artifactResolver.snapshotNormalization", false);
        LocalRepository localRepo = new LocalRepository(localRepository.toString());
        session.setLocalRepositoryManager(system.newLocalRepositoryManager(session, localRepo));
        session.setUpdatePolicy(RepositoryPolicy.UPDATE_POLICY_ALWAYS);
        DefaultMirrorSelector mirrorSelector = new DefaultMirrorSelector();
        Settings mavenSettings = getMavenSettings();
        List<Mirror> mirrors = mavenSettings.getMirrors();
        for (Mirror mirror : mirrors) {
            mirrorSelector.add(mirror.getId(), mirror.getUrl(), null, false, mirror.getMirrorOf(), "");
        }
        session.setMirrorSelector(mirrorSelector);
        return session;
    }

    private static Map<String, Authentication> getRepositoryAuthenticationsFromMavenSettings() throws SettingsBuildingException {
        Map<String, Authentication> authenticationMap = new HashMap<>();
        List<Server> servers = getMavenSettings().getServers();
        for (Server server : servers) {
            if (server.getUsername() != null && server.getPassword() != null) {
                Authentication auth = new AuthenticationBuilder().addUsername(server.getUsername()).addPassword(server.getPassword()).build();
                authenticationMap.put(server.getId(), auth);
            }
            if (server.getPassphrase() != null && server.getPrivateKey() != null) {
                Authentication auth = new AuthenticationBuilder().addPrivateKey(server.getPrivateKey(), server.getPassphrase()).build();
                authenticationMap.put(server.getId(), auth);
            }
        }
        return authenticationMap;
    }

    public static void main(String args[]) {
        MavenArtifact artifact = new MavenArtifact();
        artifact.setGroupId("de.julielab");
        artifact.setArtifactId("jcore-jsbd-ae");
        artifact.setVersion("2.3.0-SNAPSHOT");
        artifact.setPackaging("jar");
        try {
            storeArtifactWithDependencies(artifact, new File("testdir"));
        } catch (MavenException e) {
            e.printStackTrace();
        }
    }

    public static MavenArtifact getArtifactByAether(MavenArtifact artifact) throws MavenException {
        return getArtifactByAether(artifact,
                new File(System.getProperty("user.home") + File.separatorChar + Maven.LOCAL_REPO));
    }

    public static MavenArtifact getArtifactByAether(MavenArtifact a, File localRepository) throws MavenException {
        Artifact artifact = null;
        RepositorySystem repositorySystem;
        RepositorySystemSession session;
        ArtifactRequest artifactRequest;
        try {
            repositorySystem = newRepositorySystem();
            session = newSession(repositorySystem, localRepository);
            artifact = new DefaultArtifact(a.getGroupId(), a.getArtifactId(), a.getClassifier(), a.getPackaging(), a.getVersion());
            artifactRequest = new ArtifactRequest();
            artifactRequest.setArtifact(artifact);


            List<RemoteRepository> repositories = getEffectiveRepositories(session);


            artifactRequest.setRepositories(repositories);

            File localArtifactFile = new File(localRepository.getAbsolutePath() + File.separator + session.getLocalRepositoryManager().getPathForLocalArtifact(artifact));

            if (!localArtifactFile.exists()) {
                ArtifactResult artifactResult = repositorySystem.resolveArtifact(session, artifactRequest);
                artifact = artifactResult.getArtifact();
            } else {
                MavenArtifact ret = new MavenArtifact(artifact);
                ret.setFile(localArtifactFile);
                return ret;
            }
        } catch (ArtifactResolutionException e) {
            e.printStackTrace();
            throw new MavenException(e);
        } catch (SettingsBuildingException e) {
            e.printStackTrace();
        }
        return new MavenArtifact(artifact);
    }

    private static List<RemoteRepository> getEffectiveRepositories(RepositorySystemSession session) throws SettingsBuildingException {
        Map<String, Authentication> authenticationMap = getRepositoryAuthenticationsFromMavenSettings();
        DefaultRemoteRepositoryManager remoteRepositoryManager = new DefaultRemoteRepositoryManager();
        List<RemoteRepository> repositories = remoteRepositoryManager.aggregateRepositories(session, Arrays.asList(central), getRemoteRepositoriesFromSettings(), true);
        repositories = repositories.stream().map(repo -> {
            if (authenticationMap.containsKey(repo.getId())) {
                return new RemoteRepository.Builder(repo).setAuthentication(authenticationMap.get(repo.getId())).build();
            }
            return repo;
        }).collect(toList());
        return repositories;
    }

    public static void storeArtifactWithDependencies(MavenArtifact requestedArtifact, File libDir) throws MavenException {
        log.trace("Storing artifact {} with all its dependencies to {}", requestedArtifact, libDir);
        Stream<Artifact> dependencies = getDependencies(requestedArtifact);
        if (!libDir.exists())
            libDir.mkdirs();
        Consumer<Artifact> writer = a -> {
            File destination = new File(libDir.getAbsolutePath() + File.separator + a.getFile().getName());
            try {
                log.trace("Now writing: {} to {}", a, destination);
                Files.copy(a.getFile().toPath(), destination.toPath(), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                throw new IORuntimeException(e);
            }
        };
        dependencies.forEach(writer);
    }

    public static void storeArtifactsWithDependencies(Stream<MavenArtifact> requestedArtifacts, File libDir) throws MavenException {
        log.trace("Storing artifacts {} with all its dependencies to {}", requestedArtifacts, libDir);
        Stream<Artifact> dependencies = getDependencies(requestedArtifacts);
        if (!libDir.exists())
            libDir.mkdirs();
        Consumer<Artifact> writer = a -> {
            File destination = new File(libDir.getAbsolutePath() + File.separator + a.getFile().getName());
            try {
                log.trace("Now writing: {} to {}", a, destination);
                Files.copy(a.getFile().toPath(), destination.toPath(), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                throw new IORuntimeException(e);
            }
        };
        dependencies.forEach(writer);
    }

    /**
     * Retrieves the dependency tree that has <code>requestedArtifact</code> as its root. Thus, the
     * <code>requestedArtifact</code> is resolved itself and included in the returned artifacts.
     *
     * @param requestedArtifact The Maven artifact to retrieve dependencies for.
     * @return The resolved dependencies of <code>requestedArtifact</code>, including <code>requestedArtifact</code>
     * itself.
     * @throws MavenException If an artifact cannot be found or another Maven related error occurs.
     */
    public static Stream<Artifact> getDependencies(MavenArtifact requestedArtifact) throws MavenException {
        return getDependencies(requestedArtifact, false);
    }

    /**
     * Returns all available versions of the given artifact.
     *
     * @param requestedArtifact
     * @return
     * @throws MavenException
     */
    public static Stream<String> getVersions(MavenArtifact requestedArtifact) throws MavenException {
        return getVersions(requestedArtifact, "0", String.valueOf(Integer.MAX_VALUE), true, true);
    }

    /**
     * Retrieves all versions of the given artifact - whose given version is ignored in this method - that are available
     * within the described version range.
     *
     * @param requestedArtifact
     * @param lowerBound
     * @param upperBound
     * @param lowerInclusive
     * @param upperInclusive
     * @return
     * @throws MavenException
     */
    public static Stream<String> getVersions(MavenArtifact requestedArtifact, String lowerBound, String upperBound, boolean lowerInclusive, boolean upperInclusive) throws MavenException {
        RepositorySystem repositorySystem = newRepositorySystem();
        RepositorySystemSession session;
        try {
            session = newSession(repositorySystem,
                    new File(Paths.get(System.getProperty("user.home"), Maven.LOCAL_REPO).toString()));
        } catch (SettingsBuildingException e) {
            throw new MavenException(e);
        }
        String groupId = requestedArtifact.getGroupId();
        String artifactId = requestedArtifact.getArtifactId();
        String classifier = requestedArtifact.getClassifier();
        String version = requestedArtifact.getVersion();
        String packaging = requestedArtifact.getPackaging();
        String lower = lowerInclusive ? "[" : "(";
        String upper = upperInclusive ? "]" : ")";
        String range = lower + lowerBound + ", " + upperBound + upper;
        Artifact artifact = new DefaultArtifact(groupId, artifactId, classifier, packaging, range);
        try {
            VersionRangeRequest versionRangeRequest = new VersionRangeRequest(artifact, getEffectiveRepositories(session), null);
            VersionRangeResult result = repositorySystem.resolveVersionRange(session, versionRangeRequest);
            return result.getVersions().stream().map(Version::toString);
        } catch (SettingsBuildingException e) {
            throw new MavenException(e);
        } catch (VersionRangeResolutionException e) {
            e.printStackTrace();
        }
        return Stream.empty();
    }

    /**
     * Retrieves all available versions of the given artifact and returns the newest one or null, if no version is available.
     *
     * @param requestedArtifact
     * @return
     * @throws MavenException
     */
    public static String getNewestVersion(MavenArtifact requestedArtifact) throws MavenException {
        List<String> versions = getVersions(requestedArtifact).collect(Collectors.toList());
        if (!versions.isEmpty())
            return versions.get(versions.size() - 1);
        return null;
    }

    private static Stream<Artifact> getDependencies(Stream<MavenArtifact> requestedArtifacts) throws MavenException {
        RepositorySystem repositorySystem = newRepositorySystem();
        RepositorySystemSession session;
        try {
            session = newSession(repositorySystem,
                    new File(Paths.get(System.getProperty("user.home"), Maven.LOCAL_REPO).toString()));
        } catch (SettingsBuildingException e) {
            throw new MavenException(e);
        }


        final List<Dependency> components = requestedArtifacts
                .map(a -> new DefaultArtifact(a.getGroupId(), a.getArtifactId(), a.getClassifier(), a.getPackaging(), a.getVersion()))
                .map(a -> new Dependency(a, "compile"))
                .collect(Collectors.toList());

        try {
            CollectRequest collectRequest = new CollectRequest();
            collectRequest.setDependencies(components);
            collectRequest.setRepositories(getEffectiveRepositories(session));
            CollectResult collectResult = repositorySystem.collectDependencies(session, collectRequest);
            DependencyNode node = collectResult.getRoot();

            DependencyRequest dependencyRequest = new DependencyRequest();
            dependencyRequest.setRoot(node);
            DependencyResult dependencyResult = repositorySystem.resolveDependencies(session, dependencyRequest);
            return dependencyResult.getArtifactResults().stream().map(ArtifactResult::getArtifact);
        } catch (SettingsBuildingException e) {
            e.printStackTrace();
        } catch (DependencyCollectionException e) {
            e.printStackTrace();
        } catch (DependencyResolutionException e) {
            e.printStackTrace();
        }

        return Stream.empty();
    }

    /**
     * Retrieves the dependency tree that has <code>requestedArtifact</code> as its root. Thus, the
     * <code>requestedArtifact</code> is resolved itself and included in the returned artifacts.
     *
     * @param requestedArtifact The Maven artifact to retrieve dependencies for.
     * @return The resolved dependencies of <code>requestedArtifact</code>, including <code>requestedArtifact</code>
     * itself.
     * @throws MavenException If an artifact cannot be found or another Maven related error occurs.
     */
    private static Stream<Artifact> getDependencies(MavenArtifact requestedArtifact, boolean recursiveCall) throws MavenException {
        RepositorySystem repositorySystem = newRepositorySystem();
        RepositorySystemSession session;
        try {
            session = newSession(repositorySystem,
                    new File(Paths.get(System.getProperty("user.home"), Maven.LOCAL_REPO).toString()));
        } catch (SettingsBuildingException e) {
            throw new MavenException(e);
        }

        String groupId = requestedArtifact.getGroupId();
        String artifactId = requestedArtifact.getArtifactId();
        String classifier = requestedArtifact.getClassifier();
        String version = requestedArtifact.getVersion();
        String packaging = requestedArtifact.getPackaging();
        Artifact artifact = new DefaultArtifact(groupId, artifactId, classifier, packaging, version);

        Dependency dependency =
                new Dependency(artifact, "compile");

        DependencyResult dependencyResult = null;
        try {
            CollectRequest collectRequest = new CollectRequest();
            collectRequest.setRoot(dependency);
            collectRequest.setRepositories(getEffectiveRepositories(session));
            CollectResult collectResult = repositorySystem.collectDependencies(session, collectRequest);
            DependencyNode node = collectResult.getRoot();

            DependencyRequest dependencyRequest = new DependencyRequest();
            dependencyRequest.setRoot(node);
            dependencyResult = repositorySystem.resolveDependencies(session, dependencyRequest);
        } catch (DependencyCollectionException | DependencyResolutionException | SettingsBuildingException e) {
            if (!recursiveCall) {
                // EF 2018/05/13 - it seems to help to just do it again. I have no idea why. Perhaps this is just
                // BS.
                try {
                    return getDependencies(requestedArtifact, true);
                } catch (MavenException e1) {
                    throw new MavenException(e);
                }
            } else {
                throw new MavenException(e);
            }
        }

        return dependencyResult.getArtifactResults().stream().map(ArtifactResult::getArtifact);
    }

    private static List<RemoteRepository> getRemoteRepositoriesFromSettings() throws SettingsBuildingException {
        Settings effectiveSettings = getMavenSettings();

        Map<String, Authentication> authenticationMap = getRepositoryAuthenticationsFromMavenSettings();
        Map<String, Profile> profilesMap = effectiveSettings.getProfilesAsMap();
        List<RemoteRepository> remotes = new ArrayList<>(20);
        for (String profileName : effectiveSettings.getActiveProfiles()) {
            Profile profile = profilesMap.get(profileName);
            if (profile != null) {
                List<Repository> repositories = profile.getRepositories();
                for (Repository repo : repositories) {
                    Authentication auth = authenticationMap.get(repo.getId());
                    RemoteRepository.Builder repoBuilder = new RemoteRepository.Builder(repo.getId(), "default", repo.getUrl());
                    if (auth != null)
                        repoBuilder.setAuthentication(auth);
                    repoBuilder.setSnapshotPolicy(new RepositoryPolicy(true, RepositoryPolicy.UPDATE_POLICY_ALWAYS, RepositoryPolicy.CHECKSUM_POLICY_WARN));
                    RemoteRepository remoteRepo
                            = repoBuilder.build();
                    remotes.add(remoteRepo);
                }
            }
        }
        if (log.isTraceEnabled()) {
            remotes.forEach(r -> log.trace("Getting repository from Maven settings: {}", r));
        }
        return remotes;
    }

    /**
     * Returns the effective settings after resolving global and user settings.
     *
     * @return
     * @throws SettingsBuildingException
     */
    private static Settings getMavenSettings() throws SettingsBuildingException {
        String userHome = System.getProperty("user.home");
        File userMavenConfigurationHome = new File(userHome, ".m2");
        String envM2Home = System.getenv("M2_HOME");
        File DEFAULT_USER_SETTINGS_FILE = new File(userMavenConfigurationHome, "settings.xml");
        File DEFAULT_GLOBAL_SETTINGS_FILE =
                new File(System.getProperty("maven.home", envM2Home != null ? envM2Home : ""), "conf/settings.xml");


        SettingsBuildingRequest settingsBuildingRequest = new DefaultSettingsBuildingRequest();
        settingsBuildingRequest.setSystemProperties(System.getProperties());
        settingsBuildingRequest.setUserSettingsFile(DEFAULT_USER_SETTINGS_FILE);
        settingsBuildingRequest.setGlobalSettingsFile(DEFAULT_GLOBAL_SETTINGS_FILE);

        SettingsBuildingResult settingsBuildingResult;
        DefaultSettingsBuilderFactory mvnSettingBuilderFactory = new DefaultSettingsBuilderFactory();
        DefaultSettingsBuilder settingsBuilder = mvnSettingBuilderFactory.newInstance();
        settingsBuildingResult = settingsBuilder.build(settingsBuildingRequest);

        return settingsBuildingResult.getEffectiveSettings();
    }

}
