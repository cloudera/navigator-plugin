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

package com.cloudera.nav.sdk.client;

import com.cloudera.nav.sdk.model.MD5IdGenerator;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MEndPoint;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.cloudera.nav.sdk.model.annotations.MRelation;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.cloudera.nav.sdk.model.relations.RelationRole;

/**
 * Represents a template defined by a script in a custom DSL
 */

@MClass(model="cust_op")
public class CustomOperation extends Entity {

  @MRelation(role = RelationRole.PHYSICAL)
  private Entity pigOperation;
  @MProperty
  private String script;

  public CustomOperation() {
    setEntityType(EntityType.OPERATION);
    setSourceType(SourceType.SDK);
  }

  /**
   * Extend to include all fields that uniquely determine a custom entity
   */
  @Override
  public String generateId() {
    return MD5IdGenerator.generateIdentity(getSourceId(), getNamespace(),
        getName(), getOwner());
  }

  public Entity getPigOperation() {
    return pigOperation;
  }

  public void setPigOperation(Entity pigOperation) {
    this.pigOperation = pigOperation;
  }

  public String getScript() {
    return script;
  }

  public void setScript(String script) {
    this.script = script;
  }
}
