package de.julielab.jcore.pipeline.builder.base.main;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = GitHubRepository.class, name = "GitHubRepository"),
})
public class ComponentRepository implements Serializable {

    private String name;
    private String version;
    private boolean updateable;
    protected String type;

    public ComponentRepository(String name, String version, boolean updateable, String type) {
        this.name = name;
        this.version = version;
        this.updateable = updateable;
        this.type = type;
    }

    public ComponentRepository() {
        this.type = "ComponentRepository";
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    /**
     * Indicates whether the repository can be scanned for updates. This should work, for example, for {@link GitHubRepository} objects.
     * It might not work for manually assembled repositories of private components.
     *
     * @return Whether this repository can be updated.
     */
    public boolean isUpdateable() {
        return updateable;
    }

    public void setUpdateable(boolean updateable) {
        this.updateable = updateable;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "ComponentRepository{" +
                "name='" + name + '\'' +
                ", version='" + version + '\'' +
                '}';
    }
}
