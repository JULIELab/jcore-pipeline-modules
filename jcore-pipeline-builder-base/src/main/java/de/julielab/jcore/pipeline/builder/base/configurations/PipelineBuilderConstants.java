package de.julielab.jcore.pipeline.builder.base.configurations;

import java.io.File;
import java.util.StringJoiner;

/**
 * This class stores several constants organized in sub-classes.
 *
 * @author Franz Matthies <franz.matthies @ uni-jena.de>
 * @version 1.0
 * @since 1.0
 */
public class PipelineBuilderConstants {

    /**
     * This class holds all {@code GitHub} related information.
     */
    public static final class GitHub {
        public static final String SCHEME = "https";
        public static final String API = "api.github.com";
        public static final String API_REPO = "repos";
        public static final String API_CONTENT = "contents";
        public static final String RAW = "raw.githubusercontent.com";
        public static final String USER = "JULIELab";
        public static final String API_TYPE = "type";
        public static final String API_FOLDER = "dir";
        public static final String API_PATH = "path";
        public static final String API_VERSION_PARAMETER = "ref";
        public static final String API_BRANCHES = "branches";
    }

    /**
     * This class holds constants that has to do with the project specific {@code MetaFile}.
     */
    public static final class JcoreMeta {
        public static final String LOCAL_STORAGE = new StringJoiner(File.separator).add(System.getProperty("user.home"))
                .add(".jcore-pipeline-builder").toString();
        public static final String REPOSITORIES = "repositories.json";
        public static final String FILE = "component.meta";
        public static final String EXPOSABLE = "exposable";
        public static final String MAVEN_ARTIFACT = "maven-artifact";
        public static final String DESCRIPTOR_LOCATION = "descriptor-location";
        public static final String NAME = "name";
        public static final String GROUP = "group";
        public static final String CATEGORY = "category";
        public static final String CATEGORY_AE = "ae";
        public static final String CATEGORY_CONSUMER = "consumer";
        public static final String CATEGORY_READER = "reader";
        public static final String CATEGORY_MULTIPLIER = "multiplier";
        public static final String MODULE = "module";
        public static final String BASE = "base";

        public enum Category {reader, ae, consumer, multiplier}
    }

    /**
     * This class holds all {@code Maven} related information.
     */
    public static final class Maven {
        public static final String LOCAL_REPO = new StringJoiner(File.separator).add(".m2").add("repository").toString();
        public static final String SONATYPE_REPO_ID = "sonatype-nexus";
        public static final String SONATYPE_REPO_TYPE = "default";
        public static final String SONATYPE_REPO_URL = "https://oss.sonatype.org/content/repositories/releases/";
        public static final String SONATYPE_SNAPSHOT_REPO_URL = "https://oss.sonatype.org/content/repositories/snapshots/";
        public static final CharSequence POM = "pom.xml";
    }

    /**
     * This class holds constants that are {@code descriptor xml} related.
     */
    public static final class Descriptor {
        public static final String CAPABILITIES = "capabilities";
        public static final String CAPABILITIES_IN = "in";
        public static final String CAPABILITIES_OUT = "out";
    }

    public static final class UIMA {
        public static final String TYPE_STRING = "String";
        public static final String TYPE_BOOL = "Boolean";
        public static final String TYPE_INT = "Integer";
        public static final String TYPE_FLOAT = "Float";
    }
}
