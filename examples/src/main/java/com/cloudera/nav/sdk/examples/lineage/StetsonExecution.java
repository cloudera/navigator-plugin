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
import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.cloudera.nav.sdk.model.annotations.MRelation;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.cloudera.nav.sdk.model.entities.PigOperationExecution;
import com.cloudera.nav.sdk.model.relations.RelationRole;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.apache.commons.lang.StringUtils;
import org.joda.time.Instant;

/**
 * Represents a specific execution of a hypothetical custom application
 * represented by a StetsonScript
 */
@MClass(model = "stetson_exec")
public class StetsonExecution extends Entity {

  @MRelation(role = RelationRole.TEMPLATE)
  private StetsonScript template;
  @MProperty
  private Instant started;
  @MProperty
  private Instant ended;
  @MRelation(role = RelationRole.PHYSICAL)
  private Entity pigExecution; // MD5(pig.script.id) from the job conf
  @MProperty
  private String link;
  @MProperty
  private int index;
  @MProperty
  private String steward;

  public StetsonExecution(String namespace) {
    // Because the namespace is given to input/output we ensure it
    // exists when it is used by adding it as a c'tor parameter
    Preconditions.checkArgument(StringUtils.isNotEmpty(namespace));
    setNamespace(namespace);
  }

  /**
   * The execution is uniquely identified by the the template's id and
   * the external application's identifier
   */
  @Override
  public String generateId() {
    if (!Strings.isNullOrEmpty(getPigExecution().getIdentity())) {
      return CustomIdGenerator.generateIdentity(getNamespace(),
          getTemplate().getIdentity(),
          getPigExecution().getIdentity());
    } else {
      return CustomIdGenerator.generateIdentity(getNamespace(),
          getTemplate().getIdentity());
    }
  }

  @Override
  public SourceType getSourceType() {
    return SourceType.SDK;
  }

  /**
   * Stetson executions are defined to be operation execution entities
   */
  @Override
  public EntityType getEntityType() {
    return EntityType.OPERATION_EXECUTION;
  }

  public String getLink() {
    return link;
  }

  /**
   * The custom DSL template
   */
  public StetsonScript getTemplate() {
    return template;
  }

  /**
   * The Pig execution id
   */
  public Entity getPigExecution() {
    return pigExecution;
  }

  /**
   * Start time of execution in milliseconds since epoch
   */
  public Instant getStarted() {
    return started;
  }

  /**
   * End time of execution in milliseconds since epoch
   */
  public Instant getEnded() {
    return ended;
  }

  public void setTemplate(StetsonScript template) {
    this.template = template;
  }

  public void setPigExecution(Entity pigExecution) {
    this.pigExecution = pigExecution;
  }

  public void setStarted(Instant started) {
    this.started = started;
  }

  public void setEnded(Instant ended) {
    this.ended = ended;
  }

  public void setLink(String link) {
    this.link = link;
  }

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public String getSteward() {
    return steward;
  }

  public void setSteward(String steward) {
    this.steward = steward;
  }
}
