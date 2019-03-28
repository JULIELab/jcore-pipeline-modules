package de.julielab.jcore.pipeline.builder.base.main;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

public abstract class MetaDescriptionPipelineStorageMixin {
    @JsonIgnore
    // The field must match the field name in MetaDescription, not the JsonProperty name.
    public List<Description> descriptorList;
}
