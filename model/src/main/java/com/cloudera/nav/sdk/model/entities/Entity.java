/*
 * Copyright (c) 2015 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.nav.sdk.model.entities;

import com.cloudera.nav.sdk.model.IdAttrs;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Map;

import org.joda.time.Instant;

/**
 * An abstract base class for creating entities. Concrete sub-classes
 * that need to be persisted should have the @MClass annotation.
 * Only fields with getter methods annotated with @MProperty will be written.
 * Relationships with other Entities should be expressed in the form of a
 * field with a getter method annotated by @MRelation
 *
 * Hadoop component Entities will inherit from Entity while custom application
 * entities should inherit from CustomEntity
 */
public class Entity extends IdAttrs {

  public static final CharSequence ID_SEPARATOR = "##";

  // required properties
  @MProperty
  private String identity;
  @MProperty(required=true)
  private SourceType sourceType;

  @JsonProperty("type")
  @MProperty(required=true, attribute = "type")
  private EntityType entityType;

  @MProperty
  private String namespace;
  @MProperty
  private String sourceId;
  @MProperty(attribute = "originalName")
  private String name;
  @MProperty(attribute = "name")
  private String alias;
  @MProperty
  private boolean deleted;
  @MProperty
  private Long deleteTime;
  @MProperty
  private TagChangeSet tags;
  @MProperty
  private UDPChangeSet properties;
  @MProperty
  private Instant created;
  @MProperty
  private String owner;
  @MProperty
  private String description;
  @MProperty
  private String parentPath;

  private Boolean isIdGenerated = true;

  public String generateId() {
    return identity;
  }

  /**
   * @return id for this custom entity
   */
  public String getIdentity() {
    return identity;
  }

  /**
   * Set the identity for the custom entity
   * @param identity
   */
  public void setIdentity(String identity) {
    this.identity = identity;
  }

  /**
   * @return id for the designated source
   */
  public String getSourceId() {
    return sourceId;
  }

  /**
   * Set the source id for the custom entity
   * @param sourceId
   */
  public void setSourceId(String sourceId) {
    this.sourceId = sourceId;
  }

  /**
   * @return the source type
   */
  public SourceType getSourceType() {
    return sourceType;
  }

  public void setSourceType(SourceType sourceType) {
    this.sourceType = sourceType;
  }

  /**
   * @return the type of the custom entity
   */
  public EntityType getEntityType() {
    return entityType;
  }

  public void setEntityType(EntityType entityType) {
    this.entityType = entityType;
  }

  /**
   * @return
   */
  public String getNamespace() {
    return namespace;
  }

  /**
   * Set the namespace of this custom entity
   * @param namespace
   */
  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  /**
   * @return name of the custom entity
   */
  public String getName() {
    return name;
  }

  /**
   * Set the name of the custom entity
   * @param name
   */
  public void setName(String name) {
    this.name = name;
  }


  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  /**
   * Get the path of the parent of this entity.
   * The parent path means different things depending on the
   * entity. For an HDFS entity, the parent path indicates
   * the containing directory. For a Hive table, the parent path
   * is the database.
   */
  public String getParentPath() {
    return parentPath;
  }

  public void setParentPath(String parentPath) {
    this.parentPath = parentPath;
  }

  /**
   * @return true if the custom entity should be marked deleted
   */
  public boolean isDeleted() {
    return deleted;
  }

  /**
   * Set whether the custom entity should be marked as deleted.
   * @param deleted
   */
  public void setDeleted(boolean deleted) {
    this.deleted = deleted;
  }

  /**
   * @return deletion time in milliseconds since epoch if the custom entity
   *         has been deleted and null otherwise.
   */
  public Long getDeleteTime() {
    return deleteTime;
  }

  /**
   * Set deletion time for the custom entity
   * @param deleteTime
   */
  public void setDeleteTime(Long deleteTime) {
    this.deleteTime = deleteTime;
  }

  public Instant getCreated() {
    return created;
  }

  public void setCreated(Instant creationTime) {
    this.created = creationTime;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  /**
   * @return added, removed, and override tags
   */
  public TagChangeSet getTags() {
    if (tags == null) {
      tags = new TagChangeSet();
    }
    return tags;
  }

  /**
   * Override tags for entity
   * @param tags
   */
  public void setTags(String...tags) {
    setTags(Sets.newHashSet(tags));
  }

  /**
   * Override tags for entity
   * @param tags
   */
  public void setTags(Collection<String> tags) {
    getTags().setTags(tags);
  }

  /**
   * Append new tags for entity without removing existing tags
   * @param tags
   */
  public void addTags(String... tags) {
    addTags(Sets.newHashSet(tags));
  }

  /**
   * Append new tags for entity without removing existing tags
   * @param tags
   */
  public void addTags(Collection<String> tags) {
    getTags().appendTags(tags);
  }

  /**
   * Remove existing tags
   * @param tags
   */
  public void removeTags(String...tags) {
    removeTags(Sets.newHashSet(tags));
  }

  /**
   * Remove existing tags
   * @param tags
   */
  public void removeTags(Collection<String> tags) {
    getTags().removeTags(tags);
  }

  /**
   * @return new, removed, and override properties
   */
  public UDPChangeSet getProperties() {
    if (properties == null) {
      properties = new UDPChangeSet();
    }
    return properties;
  }

  /**
   * Replace existing user-defined properties
   * @param properties
   */
  public void setProperties(Map<String, String> properties) {
    getProperties().setProperties(properties);
  }

  /**
   * Add/update user-defined properties
   * @param properties
   */
  public void addProperties(Map<String, String> properties) {
    getProperties().addProperties(properties);
  }

  /**
   * Remove existing user-defined properties
   * @param keys
   */
  public void removeProperties(Collection<String> keys) {
    getProperties().removeProperties(keys);
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Entity)) return false;

    Entity entity = (Entity) o;

    // TODO if identity is null?
    if (!identity.equals(entity.identity)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return identity.hashCode();
  }

  public Boolean getIsIdGenerated() { return isIdGenerated; }

  public void setIsIdGenerated(Boolean isIdGenerated) {
    this.isIdGenerated = isIdGenerated;
  }
}
