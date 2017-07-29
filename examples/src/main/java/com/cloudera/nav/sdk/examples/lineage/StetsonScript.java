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

package com.cloudera.nav.sdk.examples.lineage;

import com.cloudera.nav.sdk.model.IdAttrs;
import com.cloudera.nav.sdk.model.CustomIdGenerator;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MRelation;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.cloudera.nav.sdk.model.entities.PigOperation;
import com.cloudera.nav.sdk.model.relations.RelationRole;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.UUID;

import org.apache.commons.lang.StringUtils;

/**
 * Represents a template defined by a script in a hypothetical custom DSL
 */
@MClass(model = "stetson_op")
public class StetsonScript extends Entity {

  @MRelation(role = RelationRole.PHYSICAL)
  private Entity pigOperation;

  public StetsonScript(String namespace) {
    // Because the namespace is given to input/output we ensure it
    // exists when it is used by adding it as a c'tor parameter
    Preconditions.checkArgument(StringUtils.isNotEmpty(namespace));
    setNamespace(namespace);
  }

  /**
   * The script template is uniquely defined by the name and the owner
   */
  @Override
  public String generateId() {
    if (!Strings.isNullOrEmpty(getPigOperation().getIdentity())) {
      return CustomIdGenerator.generateIdentity(getNamespace(),
          getPigOperation().getIdentity());
    } else {
      return CustomIdGenerator.generateIdentity(getNamespace(),
          UUID.randomUUID().toString());
    }
  }

  @Override
  public SourceType getSourceType() {
    return SourceType.SDK;
  }

  /**
   * The StetsonScript represents a template and is therefore always an
   * OPERATION entity
   */
  @Override
  public EntityType getEntityType() {
    return EntityType.OPERATION;
  }

  /**
   * The StetsonScript is linked to a PIG operation via a Logical-Physical
   * relationship where the Pig operation is the PHYSICAL node
   */
  public Entity getPigOperation() {
    return pigOperation;
  }

  public void setPigOperation(Entity pigOperation) {
    this.pigOperation = pigOperation;
  }
}
