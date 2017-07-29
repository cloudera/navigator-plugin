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
package com.cloudera.nav.sdk.client.writer;

import com.cloudera.nav.sdk.client.ClientConfig;
import com.cloudera.nav.sdk.client.writer.registry.MClassRegistry;
import com.cloudera.nav.sdk.client.writer.registry.MRelationEntry;
import com.cloudera.nav.sdk.model.annotations.MEndPoint;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.relations.Relation;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;

import org.apache.commons.lang.StringUtils;

/**
 * Responsible for writing a given metadata entity and implied relations
 * to the underlying storage. Currently supports HDFS and local file system
 */
public abstract class MetadataWriter {

  protected final ClientConfig config;
  protected final OutputStream stream;
  protected final MClassRegistry registry;

  public MetadataWriter(ClientConfig config, OutputStream stream) {
    this.config = config;
    this.stream = stream;
    registry = new MClassRegistry(config.getNamespace());
  }

  /**
   * Write the given entity based on @MProperty annotations.
   * Also create and write relations based on @MRelation annotations.
   * @param entity
   */
  public void write(Entity entity) {
    write(Collections.singleton(entity));
  }

  public void write(Collection<Entity> entities) {
    MClassWrapper mclassWrapper = new MClassWrapper();
    mclassWrapper.setAutocommit(config.isAutocommit());
    for (Entity entity : entities) {
      Preconditions.checkNotNull(entity);
      getAllMClasses(entity, mclassWrapper);
    }
    persistMetadataValues(mclassWrapper);
  }

  public void writeRelation(Relation relation) {
    writeRelations(Collections.singleton(relation));
  }

  public void writeRelations(Collection<Relation> relations) {
    MClassWrapper mClassWrapper = new MClassWrapper();
    mClassWrapper.setAutocommit(config.isAutocommit());
    mClassWrapper.addRelations(relations);
    persistMetadataValues(mClassWrapper);
  }

  protected abstract void persistMetadataValues(MClassWrapper graph);

  /**
   * Flush the data that has been written but still not yet persisted
   */
  public void flush() {
    try {
      stream.flush();
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  /**
   * Close this writer
   */
  public void close() {
    try {
      stream.close();
    } catch (IOException e) {
      Throwables.propagate(e);
    }
    registry.reset();
  }

  /**
   * @return latest ResultSet from the most recent write operation
   */
  public abstract ResultSet getLastResultSet();

  private void getAllMClasses(Entity entity, MClassWrapper graph) {
    if (StringUtils.isEmpty(entity.getIdentity())) {
      entity.setIdentity(entity.generateId());
    }

    registry.validateRequiredMProperties(entity);
    if (!graph.hasEntity(entity)) {
      graph.addEntity(entity);
      getMRelations(entity, graph);
    }
  }

  private void getMRelations(Entity entity, MClassWrapper graph) {
    // get all MRelation entries
    Collection<MRelationEntry> relationAttrs = registry.getRelations(
        entity.getClass());
    for (MRelationEntry relEntry : relationAttrs) {
      for(Entity other : relEntry.getConnectedEntities(entity)) {
        if (other.getClass().getAnnotation(MEndPoint.class) == null) {
          getAllMClasses(other, graph);
        }
      }
      // add Relation after doing the getAllMClasses call so the connected
      // entity id's have all been generated if necessary
      graph.addRelation(relEntry.buildRelation(entity, config.getNamespace()));
    }
  }
}
