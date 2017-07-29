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

package com.cloudera.nav.sdk.model;

import static org.junit.Assert.assertTrue;

import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.entities.HdfsEntity;
import com.cloudera.nav.sdk.model.entities.HiveColumn;
import com.cloudera.nav.sdk.model.entities.HiveTable;
import com.cloudera.nav.sdk.model.entities.PigOperation;
import com.cloudera.nav.sdk.model.entities.PigOperationExecution;
import com.google.common.collect.ImmutableList;

import java.util.Collection;

import org.junit.Test;

public class MClassTest {

  @Test
  public void testDefaultMClassEntities() {
    Collection<Class<? extends Entity>> classes = ImmutableList.of(
        PigOperation.class, HdfsEntity.class, HiveColumn.class,
        HiveTable.class, PigOperationExecution.class);
    for (Class<? extends Entity> aClass : classes) {
      assertTrue(aClass.isAnnotationPresent(MClass.class));
    }
  }
}
