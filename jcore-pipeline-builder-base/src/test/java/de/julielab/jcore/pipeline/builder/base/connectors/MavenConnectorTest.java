package de.julielab.jcore.pipeline.builder.base.connectors;

import de.julielab.jcore.pipeline.builder.base.exceptions.MavenException;
import de.julielab.jcore.pipeline.builder.base.main.MavenArtifact;
import org.eclipse.aether.artifact.Artifact;
import org.junit.Test;

import java.util.stream.Stream;

import static org.junit.Assert.*;

public class MavenConnectorTest {

    @Test
    public void getArtifactByAether() throws MavenException {
        //de.julielab:jcore-opennlp-chunk-ae:jar:2.3.0-SNAPSHOT
//        MavenArtifact mavenArtifact = new MavenArtifact();
//        mavenArtifact.setArtifactId("jcore-opennlp-chunk-ae");
//        mavenArtifact.setGroupId("de.julielab");
//        mavenArtifact.setVersion("2.3.0-SNAPSHOT");
//        mavenArtifact.setPackaging("jar");
//        MavenConnector.getVersions(mavenArtifact).forEach(System.out::println);
    }
}