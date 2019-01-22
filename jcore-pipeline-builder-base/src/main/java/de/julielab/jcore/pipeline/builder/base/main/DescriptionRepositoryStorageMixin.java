package de.julielab.jcore.pipeline.builder.base.main;

import com.fasterxml.jackson.annotation.JsonIgnore;

public abstract class DescriptionRepositoryStorageMixin {
    @JsonIgnore
    abstract boolean isActive();
}
