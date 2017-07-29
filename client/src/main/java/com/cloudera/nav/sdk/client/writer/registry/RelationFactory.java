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
package com.cloudera.nav.sdk.client.writer.registry;

import com.cloudera.nav.sdk.model.IdAttrs;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.relations.DataFlowRelation;
import com.cloudera.nav.sdk.model.relations.InstanceOfRelation;
import com.cloudera.nav.sdk.model.relations.LogicalPhysicalRelation;
import com.cloudera.nav.sdk.model.relations.ParentChildRelation;
import com.cloudera.nav.sdk.model.relations.Relation;
import com.cloudera.nav.sdk.model.relations.RelationIdGenerator;
import com.cloudera.nav.sdk.model.relations.RelationRole;
import com.cloudera.nav.sdk.model.relations.RelationType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.Collection;

/**
 * Creates Relation instances from the information in MRelationEntry
 */
public class RelationFactory {

  public Relation createRelation(RelationType type, Entity entity,
                                 Collection<? extends Entity> other,
                                 RelationRole roleOfOther, String namespace) {
    switch(type) {
      case DATA_FLOW:
        return createDataFlowRelation(roleOfOther, entity, other, namespace);
      case PARENT_CHILD:
        return createParentChildRelation(roleOfOther, entity, other, namespace);
      case LOGICAL_PHYSICAL:
        return createLogicalPhysicalRelation(roleOfOther, entity, other,
            namespace);
      case INSTANCE_OF:
        return createInstanceOfRelation(roleOfOther, entity, other, namespace);
      default:
        throw new IllegalArgumentException("Invalid RelationType " +
            type.toString());
    }
  }

  @SuppressWarnings("unchecked")
  private Relation createDataFlowRelation(RelationRole roleOfOther,
                                          Entity entity,
                                          Collection<? extends Entity> other,
                                          String namespace) {
    DataFlowRelation.Builder builder = DataFlowRelation.builder();
    Collection<IdAttrs> idAttrsList = getIdAttrsList(other);

    if (roleOfOther == RelationRole.SOURCE) {
      builder.target(entity).sources(other).ep1Attributes(idAttrsList);
    } else {
      builder.source(entity).targets(other).ep2Attributes(idAttrsList);
    }
    return builder.idGenerator(new RelationIdGenerator())
        .namespace(namespace).build();
  }

  @SuppressWarnings("unchecked")
  private Relation createParentChildRelation(RelationRole roleOfOther,
                                             Entity entity,
                                             Collection<? extends Entity> other,
                                             String namespace) {
    ParentChildRelation.Builder builder = ParentChildRelation.builder();
    if (roleOfOther == RelationRole.PARENT) {
      Preconditions.checkArgument(other.size() == 1,
          "Only 1 parent is allowed in a parent-child relationship");
      builder.child(entity).parent(Iterables.getOnlyElement(other));
    } else {
      builder.parent(entity).children(other);
    }
    return builder.idGenerator(new RelationIdGenerator())
        .namespace(namespace).build();
  }

  @SuppressWarnings("unchecked")
  private Relation createInstanceOfRelation(RelationRole roleOfOther,
                                            Entity entity,
                                            Collection<? extends Entity> other,
                                            String namespace) {
    Preconditions.checkArgument(other.size() == 1,
        "Only one instance and template are allowed");
    InstanceOfRelation.Builder builder = InstanceOfRelation.builder();
    if (roleOfOther == RelationRole.TEMPLATE) {
      builder.instance(entity).template(Iterables.getOnlyElement(other));
    } else {
      builder.template(entity).instance(Iterables.getOnlyElement(other));
    }
    return builder.idGenerator(new RelationIdGenerator())
        .namespace(namespace).build();
  }

  @SuppressWarnings("unchecked")
  private Relation createLogicalPhysicalRelation(
      RelationRole roleOfOther, Entity entity,
      Collection<? extends Entity> other, String namespace) {

    Collection<IdAttrs> idAttrsList = getIdAttrsList(other);
    LogicalPhysicalRelation.Builder builder = LogicalPhysicalRelation.builder();
    if (roleOfOther == RelationRole.LOGICAL) {
      Preconditions.checkArgument(other.size() == 1,
          "Only 1 logical allowed in each logical-physical relationship");
      builder.physical(entity).logical(Iterables.getOnlyElement(other))
          .ep1Attributes(idAttrsList);
    } else {
      builder.logical(entity).physical(other).ep2Attributes(idAttrsList);
    }
    return builder.idGenerator(new RelationIdGenerator())
        .namespace(namespace).build();
  }

  private Collection<IdAttrs> getIdAttrsList(
      Collection<? extends Entity> other) {
    Collection<IdAttrs> idAttrsList = Lists.newArrayList();
    for (Entity en : other) {
      if (!en.getIsIdGenerated()) {
        IdAttrs at = new IdAttrs();
        en.populateIdAttrs(at);
        idAttrsList.add(at);
      }
    }

    return idAttrsList;
  }
}
