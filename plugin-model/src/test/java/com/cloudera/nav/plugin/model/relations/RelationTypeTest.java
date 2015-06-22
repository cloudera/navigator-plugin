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

import org.junit.Test;

public class RelationTypeTest {

  @Test
  public void testDataFlow() {
    assertEquals(RelationType.DATA_FLOW.getEndpoint1Role(),
        RelationRole.SOURCE);
    assertEquals(RelationType.DATA_FLOW.getEndpoint2Role(),
        RelationRole.TARGET);
  }

  @Test
  public void testParentChild() {
    assertEquals(RelationType.PARENT_CHILD.getEndpoint1Role(),
        RelationRole.PARENT);
    assertEquals(RelationType.PARENT_CHILD.getEndpoint2Role(),
        RelationRole.CHILD);
  }

  @Test
  public void testLogicalPhysical() {
    assertEquals(RelationType.LOGICAL_PHYSICAL.getEndpoint1Role(),
        RelationRole.LOGICAL);
    assertEquals(RelationType.LOGICAL_PHYSICAL.getEndpoint2Role(),
        RelationRole.PHYSICAL);
  }

  @Test
  public void testInstanceOf() {
    assertEquals(RelationType.INSTANCE_OF.getEndpoint1Role(),
        RelationRole.TEMPLATE);
    assertEquals(RelationType.INSTANCE_OF.getEndpoint2Role(),
        RelationRole.INSTANCE);
  }
}
