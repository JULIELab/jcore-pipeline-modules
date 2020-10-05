package de.julielab.jcore.pipeline.builder.cli.util;

import de.julielab.java.utilities.classpath.JarLoader;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

public class ArtifactLoader {
    private static final Set<File> loadedArtifacts = new HashSet<>();

    public synchronized static void loadArtifact(File file) {
        File absolute = file.getAbsoluteFile();
        if (!loadedArtifacts.contains(absolute)) {
            JarLoader.addJarToClassPath(absolute);
            loadedArtifacts.add(absolute);
        }
    }
}
