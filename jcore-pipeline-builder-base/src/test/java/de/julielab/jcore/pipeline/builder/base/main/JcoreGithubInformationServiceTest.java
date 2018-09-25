package de.julielab.jcore.pipeline.builder.base.main;

import de.julielab.jcore.pipeline.builder.base.exceptions.DescriptorLoadingException;
import de.julielab.jcore.pipeline.builder.base.exceptions.GithubInformationException;
import de.julielab.jcore.pipeline.builder.base.exceptions.MavenException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.stream.Collectors;

/**
 *  This test requires contact to GitHub via the GitHub API. However, only anonymous connection is
 * currently possible. For anonymous connections, only 60 requests per hour are allowed. Thus not usable for testing.
 * @see https://developer.github.com/v3/#rate-limiting
 */
@Ignore
public class JcoreGithubInformationServiceTest {
    private final static Logger log = LoggerFactory.getLogger(JcoreGithubInformationServiceTest.class);
    private JcoreGithubInformationService metaInf = JcoreGithubInformationService.getInstance();


    @Test
    public void saveMetaInformationToDisk() throws GithubInformationException, IOException {
        for (Integer i = 0; i < Repositories.findRepositories().length; i++) {
            metaInf.saveMetaInformationToDisk(Repositories.findRepositories()[i]);
        }
    }


    @Test
    public void loadComponentMetaInformation() throws GithubInformationException, MavenException {
        metaInf.getMetaInformation(false);
        metaInf.loadAllArtifacts();
        for (MetaDescription jobj : metaInf.getMetaInformation(false)) {
            String descriptorLocations = "<could not retrieve descriptor locations>";
            try {
                descriptorLocations = jobj.getJCoReDescriptions().stream().map(Description::getLocation).collect(Collectors.joining(", "));
            } catch (DescriptorLoadingException e) {
                log.error("Error when loading component meta information", e);
            }
            log.debug(String.format("'%s' has its descriptor at '%s' with a Maven Artifact of '%s'",
                    jobj.getName(),
                    descriptorLocations,
                    jobj.getMavenArtifact().getArtifactId()));
        }
    }

}