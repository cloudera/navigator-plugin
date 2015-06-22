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

package com.cloudera.nav.plugin.model.relations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.cloudera.nav.plugin.model.entities.Entity;
import com.cloudera.nav.plugin.model.entities.HdfsEntity;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Test;

public class RelationTest {

  @Test
  public void testDataFlow() {
    HdfsEntity source = new HdfsEntity();
    String idOfSource = "sourceEntityId";
    String sourceIdOfSource = "sourceIdOfSourceEntity";
    source.setIdentity(idOfSource);
    source.setSourceId(sourceIdOfSource);
    HdfsEntity target = new HdfsEntity();
    String idOfTarget = "targetEntityId";
    String sourceIdOfTarget = "sourceIdOfTargetEntity";
    target.setIdentity(idOfTarget);
    target.setSourceId(sourceIdOfTarget);

    DataFlowRelation rel = DataFlowRelation.builder()
        .source(source)
        .target(target)
        .namespace("test")
        .idGenerator(new RelationIdGenerator())
        .build();
    assertEquals(Iterables.getOnlyElement(rel.getSourceIds()), idOfSource);
    assertEquals(Iterables.getOnlyElement(rel.getTargetIds()), idOfTarget);
    assertEquals(rel.getSourceTypeOfSource(), source.getSourceType());
    assertEquals(rel.getSourceTypeOfTarget(), target.getSourceType());
    assertEquals(rel.getType(), RelationType.DATA_FLOW);
    assertEquals(rel.getNamespace(), "test");
  }

  @Test
  public void testParentChild() {
    HdfsEntity parent = new HdfsEntity();
    String idOfParent = "parentEntityId";
    String sourceIdOfParent = "sourceIdOfParent";
    parent.setIdentity(idOfParent);
    parent.setSourceId(sourceIdOfParent);
    HdfsEntity child1 = new HdfsEntity();
    String idOfChild1 = "child1EntityId";
    String sourceIdOfChild = "sourceIdOfChildEntity";
    child1.setIdentity(idOfChild1);
    child1.setSourceId(sourceIdOfChild);
    HdfsEntity child2 = new HdfsEntity();
    String idOfChild2 = "child1EntityId";
    child2.setIdentity(idOfChild2);
    child2.setSourceId(sourceIdOfChild);

    ParentChildRelation rel = ParentChildRelation.builder()
        .parent(parent)
        .children(ImmutableList.<Entity>of(child1, child2))
        .namespace("test")
        .idGenerator(new RelationIdGenerator())
        .build();
    assertEquals(rel.getParentId(), idOfParent);
    assertEquals(Sets.newHashSet(rel.getChildrenIds()), Sets.newHashSet(
        idOfChild1, idOfChild2));
    assertEquals(rel.getSourceTypeOfParent(), parent.getSourceType());
    assertEquals(rel.getSourceTypeOfChildren(), child2.getSourceType());
    assertEquals(rel.getType(), RelationType.PARENT_CHILD);
    assertEquals(rel.getNamespace(), "test");
  }

  @Test
  public void testLogicalPhysical() {
    HdfsEntity logical = new HdfsEntity();
    String idOfLogical = "logicalEntityId";
    String sourceIdOfLogical = "sourceIdOfLogicalEntity";
    logical.setIdentity(idOfLogical);
    logical.setSourceId(sourceIdOfLogical);
    HdfsEntity physical1 = new HdfsEntity();
    String idOfPhysical = "physicalEntityId";
    String sourceIdOfPhysical = "sourceIdOfPhysicalEntity";
    physical1.setIdentity(idOfPhysical);
    physical1.setSourceId(sourceIdOfPhysical);
    HdfsEntity physical2 = new HdfsEntity();
    String idOfPhysical2 = "physical2EntityId";
    physical2.setIdentity(idOfPhysical2);
    physical2.setSourceId(sourceIdOfPhysical);

    LogicalPhysicalRelation rel = LogicalPhysicalRelation.builder()
        .logical(logical)
        .physical(ImmutableList.<Entity>of(physical1, physical2))
        .namespace("test")
        .idGenerator(new RelationIdGenerator())
        .build();
    assertEquals(rel.getLogicalId(), idOfLogical);
    assertTrue(CollectionUtils.isEqualCollection(
        Sets.newHashSet(rel.getPhysicalIds()),
        Sets.newHashSet(idOfPhysical, idOfPhysical2)));
    assertEquals(rel.getSourceTypeOfLogical(), logical.getSourceType());
    assertEquals(rel.getSourceTypeOfPhysical(), physical2.getSourceType());
    assertEquals(rel.getType(), RelationType.LOGICAL_PHYSICAL);
    assertEquals(rel.getNamespace(), "test");
  }

  @Test
  public void testInstanceOf() {
    HdfsEntity template = new HdfsEntity();
    String idOfTemplate = "templateEntityId";
    String sourceIdOfTemplate = "sourceIdOfTemplateEntity";
    template.setIdentity(idOfTemplate);
    template.setSourceId(sourceIdOfTemplate);
    HdfsEntity instance = new HdfsEntity();
    String idOfInstance = "instanceEntityId";
    String sourceIdOfInstance = "sourceIdOfInstanceEntity";
    instance.setIdentity(idOfInstance);
    instance.setSourceId(sourceIdOfInstance);

    InstanceOfRelation rel = InstanceOfRelation.builder()
        .template(template)
        .instance(instance)
        .namespace("test")
        .idGenerator(new RelationIdGenerator())
        .build();
    assertEquals(rel.getTemplateId(), idOfTemplate);
    assertEquals(Iterables.getOnlyElement(rel.getInstanceIds()), idOfInstance);
    assertEquals(rel.getSourceTypeOfTemplate(), template.getSourceType());
    assertEquals(rel.getSourceTypeOfInstance(), instance.getSourceType());
    assertEquals(rel.getType(), RelationType.INSTANCE_OF);
    assertEquals(rel.getNamespace(), "test");
  }
}
