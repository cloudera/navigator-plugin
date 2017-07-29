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
 * Represents a specific execution of a StetsonScript
 */

@MClass(model="cust_op_exec")
public class CustomOperationExecution extends Entity {

  @MRelation(role = RelationRole.INSTANCE)
  private CustomOperation template;
  @MRelation(role = RelationRole.PHYSICAL)
  private Entity pigExecution;

  @MProperty
  private Long startTime;
  @MProperty
  private Long endTime;

  public CustomOperationExecution() {
    setSourceType(SourceType.SDK);
    setEntityType(EntityType.OPERATION_EXECUTION);
  }

  @Override
  public String generateId() {
    return MD5IdGenerator.generateIdentity(getTemplate().getIdentity());
  }

  public CustomOperation getTemplate() {
    return template;
  }

  public void setTemplate(CustomOperation template) {
    this.template = template;
  }

  public Entity getPigExecution() {
    return pigExecution;
  }

  public void setPigExecution(Entity pigExecution) {
    this.pigExecution = pigExecution;
  }

  public Long getStartTime() {
    return startTime;
  }

  public void setStartTime(Long startTime) {
    this.startTime = startTime;
  }

  public Long getEndTime() {
    return endTime;
  }

  public void setEndTime(Long endTime) {
    this.endTime = endTime;
  }
}
