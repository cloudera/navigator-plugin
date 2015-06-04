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

package com.cloudera.nav.plugin.model.entities;

import static org.junit.Assert.assertEquals;

import com.cloudera.nav.plugin.model.MD5IdGenerator;
import com.cloudera.nav.plugin.model.Source;
import com.cloudera.nav.plugin.model.SourceType;

import org.junit.*;

public class EntityTest {

  @Test
  public void testHdfsEntity() {
    Source hdfs1 = new Source("HDFS-1", SourceType.HDFS, "Cluster",
        "http://ns1","1");
    HdfsEntity entity = new HdfsEntity();
    entity.setType(EntityType.DIRECTORY);
    entity.setFileSystemPath("/user/test");
    entity.setSourceId(hdfs1.getIdentity());
    String id = MD5IdGenerator.generateIdentity(hdfs1.getIdentity(),
        entity.getFileSystemPath());
    assertEquals(id, entity.generateId());
  }

  @Test
  public void testHiveTable() {
    Source hive1 = new Source("HIVE-1", SourceType.HIVE, "Cluster",
        "http://hive-server:port", "1");
    HiveTable entity = new HiveTable();
    entity.setDatabaseName("db");
    entity.setTableName("table");
    entity.setSourceId(hive1.getIdentity());
    String id = MD5IdGenerator.generateIdentity(hive1.getIdentity(),
        entity.getDatabaseName(), entity.getTableName());
    assertEquals(id, entity.generateId());
  }

  @Test
  public void testHiveColumn() {
    Source hive1 = new Source("HIVE-1", SourceType.HIVE, "Cluster",
        "http://hive-server:port","1");
    HiveColumn entity = new HiveColumn();
    entity.setDatabaseName("db");
    entity.setTableName("table");
    entity.setColumnName("column");
    entity.setSourceId(hive1.getIdentity());
    String id = MD5IdGenerator.generateIdentity(hive1.getIdentity(),
        entity.getDatabaseName(), entity.getTableName(),
        entity.getColumnName());
    assertEquals(id, entity.generateId());
  }
}
