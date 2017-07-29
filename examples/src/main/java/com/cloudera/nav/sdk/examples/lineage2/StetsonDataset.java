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

package com.cloudera.nav.sdk.examples.lineage2;

import com.cloudera.nav.sdk.model.IdAttrs;
import com.cloudera.nav.sdk.model.CustomIdGenerator;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MRelation;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.cloudera.nav.sdk.model.entities.HdfsEntity;
import com.cloudera.nav.sdk.model.relations.RelationRole;

import java.util.UUID;

/**
 * This is a custom logical dataset that is physically backed by an
 * HDFS directory
 */
@MClass(model = "stetson_dataset")
public class StetsonDataset extends Entity {

  @MRelation(role= RelationRole.PHYSICAL)
  private Entity hdfsEntity;

  /**
   * @param name Stetson name for the dataset
   * @param namespace
   * @param path identity of the underlying HDFS directory
   */
  public StetsonDataset(String name, String namespace, String path,
                        String sourceId) {
    setName(name);
    setNamespace(namespace);
    setHdfsEntity(sourceId, path);
  }

  @Override
  public SourceType getSourceType() {
    return SourceType.SDK;
  }

  @Override
  public EntityType getEntityType() {
    return EntityType.DATASET;
  }

  public Entity getHdfsEntity() {
    return hdfsEntity;
  }

  @Override
  public String generateId() {
    return CustomIdGenerator.generateIdentity(getName(), getNamespace(),
        UUID.randomUUID().toString());
  }

  public void setHdfsEntity(String sourceId,
                            String path) {
    hdfsEntity = new HdfsEntity(path, EntityType.DIRECTORY, sourceId);
  }
}
