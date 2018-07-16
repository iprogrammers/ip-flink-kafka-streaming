package com.softcell.domains;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Data;
import org.joda.time.DateTime;
import org.springframework.data.annotation.*;
import org.springframework.data.domain.Persistable;
import org.springframework.data.mongodb.core.index.IndexDirection;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Objects;

@Document
@Data
public abstract class AuditEntity implements Persistable<String> {
    @CreatedDate
    @JsonProperty(
            access = Access.WRITE_ONLY
    )
    private DateTime createdAt;
    @CreatedBy
    @JsonProperty(
            access = Access.WRITE_ONLY
    )
    private String createdBy;
    @JsonSerialize(using = JsonJodaDateTimeSerializer.class)
    @LastModifiedDate
    @JsonProperty(
            access = Access.READ_WRITE
    )
    @Indexed(direction = IndexDirection.DESCENDING)
    private DateTime updatedAt;
    @LastModifiedBy
    @JsonProperty(
            access = Access.WRITE_ONLY
    )
    private String updateBy;
    @Version
    @JsonProperty(
            access = Access.WRITE_ONLY
    )
    private Long version;

    @JsonIgnore
    private boolean persisted;

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (!(o instanceof AuditEntity)) {
            return false;
        } else {
            AuditEntity that = (AuditEntity) o;
            return Objects.equals(this.createdAt, that.createdAt) && Objects.equals(this.createdBy, that.createdBy) && Objects.equals(this.updatedAt, that.updatedAt) && Objects.equals(this.updateBy, that.updateBy) && Objects.equals(this.version, that.version);
        }
    }

    public int hashCode() {
        return Objects.hash(this.createdAt, this.createdBy, this.updatedAt, this.updateBy, this.version);
    }

    @JsonIgnore
    @Override
    public boolean isNew() {
        return !persisted;
    }
}