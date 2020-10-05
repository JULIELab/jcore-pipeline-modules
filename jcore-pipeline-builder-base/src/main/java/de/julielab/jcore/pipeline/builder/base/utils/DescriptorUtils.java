package de.julielab.jcore.pipeline.builder.base.utils;

import org.apache.uima.UIMAFramework;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.XMLInputSource;
import org.apache.uima.util.XMLParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class DescriptorUtils {
    private final static Logger log = LoggerFactory.getLogger(DescriptorUtils.class);

    private final static Map<String, String> descRoots = new HashMap<>() {{
        put("collectionReaderDescription", "processingResourceMetaData");
        put("analysisEngineDescription", "analysisEngineMetaData");
        put("casConsumerDescription", "processingResourceMetaData");
    }};

    /**
     * Searches the {@code artifact} for all descriptor files ending in {@code .xml} and residing directly in {@code ../desc/}.
     *
     * @param artifact
     * @throws IOException
     * @return Descriptor File as InputStream
     */
    public static Map<URI, ResourceSpecifier> searchDescriptor(File artifact) throws IOException {
        Map<URI, ResourceSpecifier> descriptors = new HashMap<>();
        XMLParser parser = UIMAFramework.getXMLParser();
        try (FileSystem zipFs = FileSystems.newFileSystem(artifact.toPath(), null)) {
            for (Path rootDir : zipFs.getRootDirectories()) {
                Iterator<Path> xmlIt = Files.walk(rootDir).filter(p -> p.getFileName().toString().toLowerCase().endsWith(".xml")).iterator();
                while (xmlIt.hasNext()) {
                    Path xmlFile = xmlIt.next();
                    try {
                        ResourceSpecifier resourceSpecifier = parser.parseResourceSpecifier(new XMLInputSource(xmlFile.toUri().toURL().openStream(), null));
                        descriptors.put(xmlFile.toUri(), resourceSpecifier);
                    } catch (InvalidXMLException e) {
                        log.debug("XML file {} could not be parsed as a UIMA descriptor and is skipped in the search of descriptors in {}", xmlFile, artifact);
                    }
                }

            }
        }

        if (descriptors.isEmpty())
            log.debug("No descriptors were found for artifact file {}", artifact);
        return descriptors;
    }

    /**
     * Searches the {@code artifact} for a descriptor that is given by {@code descLocation}.
     *
     * @param artifactPath
     * @param descLocation
     * @return
     */
    public static ResourceSpecifier searchDescriptor(File artifactPath, String descLocation) throws IOException {
        ResourceSpecifier spec = null;
        XMLParser parser = UIMAFramework.getXMLParser();
        try (ZipFile zipFile = new ZipFile(artifactPath)) {
            String path = descLocation.replaceAll("\\.", "/") + ".xml";
            ZipEntry entry = zipFile.getEntry(path);
            if (entry == null)
                throw new IllegalStateException("The meta descriptor provides the descriptor location " + descLocation + " which could not be found in " + artifactPath.getAbsolutePath());
            InputStream inputStream = zipFile.getInputStream(entry);
            spec = parser.parseResourceSpecifier(new XMLInputSource(inputStream, null));
        } catch (InvalidXMLException e) {
            log.error("Could not load XML descriptor {} in file {}", descLocation, artifactPath, e);
        }


        return spec;
    }

    public static Map<String, String> getDescRoots() {
        return descRoots;
    }
}
