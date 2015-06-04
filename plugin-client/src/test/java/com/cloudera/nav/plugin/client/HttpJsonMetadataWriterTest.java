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

package com.cloudera.nav.plugin.client;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.cloudera.nav.plugin.client.writer.HttpJsonMetadataWriter;
import com.cloudera.nav.plugin.client.writer.MetadataWriter;
import com.cloudera.nav.plugin.model.Source;
import com.cloudera.nav.plugin.model.SourceType;
import com.cloudera.nav.plugin.model.entities.Entity;
import com.cloudera.nav.plugin.model.entities.EntityType;
import com.cloudera.nav.plugin.model.entities.HdfsEntity;
import com.cloudera.nav.plugin.model.relations.Relation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.junit.*;
import org.mockito.*;

public class HttpJsonMetadataWriterTest {

  private Writer mockWriter;
  private PluginConfigurations config;
  private HttpURLConnection mockConn;

  @Before
  public void setUp() throws IOException {
    mockWriter = mock(Writer.class);
    mockConn = mock(HttpURLConnection.class);
    doReturn(200).when(mockConn).getResponseCode();
    config = mock(PluginConfigurations.class);
    doReturn("test").when(config).getNamespace();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWriteBasic() throws IOException {
    Source source = new Source("HDFS-1", SourceType.HDFS, "Cluster",
        "http://ns1", "0");
    HdfsEntity entity = new HdfsEntity();
    entity.setSourceId(source.getIdentity());
    entity.setFileSystemPath("/user/test");
    entity.setType(EntityType.DIRECTORY);
    entity.setTags(ImmutableList.of("foo", "bar"));

    HttpJsonMetadataWriter mWriter = new HttpJsonMetadataWriter(config,
        mockWriter, mockConn);
    mWriter.write(entity);

    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(mockWriter).append(captor.capture());
    String value = captor.getValue();
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> values = (Map<String, Object>)mapper.readValue(value,
        Map.class);
    assertEquals(values.get("identity"), entity.getIdentity());
    assertEquals(values.get("fileSystemPath"), entity.getFileSystemPath());
    assertEquals(values.get("sourceId"), source.getIdentity());
    assertEquals(values.get("sourceType"), SourceType.HDFS.name());
    assertEquals(values.get("type"), EntityType.DIRECTORY.name());
    assertEquals(values.get("deleted"), false);
    assertTrue(CollectionUtils.isEqualCollection(
        (Collection<String>) values.get("tags"),
        ImmutableList.of("foo", "bar")));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWriteMRelation() throws IOException {
    Source source = new Source("ExternalApp", SourceType.PLUGIN, "ExternalApp",
        "http://appHost:port", "0");
    CustomOperationExecution exec = prepExec(source);
    HttpJsonMetadataWriter mWriter = new HttpJsonMetadataWriter(config,
        mockWriter, mockConn);
    mWriter.write(exec);

    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(mockWriter, times(9)).append(captor.capture());
    ObjectMapper mapper = new ObjectMapper();
    List<Map<String, Object>> values = Lists.newArrayList();
    Set<String> markers = Sets.newHashSet("[", "]", ",");
    for (String value : captor.getAllValues()) {
      if (!markers.contains(value)) {
        values.add(mapper.readValue(value, Map.class));
      }
    }

    int relationCount = 0;
    int entityCount = 0;
    String mtype;
    for (Map<String, Object> value : values) {
      mtype = value.get(MetadataWriter.MTYPE).toString();
      if (mtype.equals(Entity.MTYPE)) {
        entityCount++;
      } else if (mtype.equals(Relation.MTYPE)) {
        relationCount++;
      } else {
        throw new AssertionError("Unrecognized metadata type " +
            String.valueOf(mtype));
      }
    }
    // custom op and exec
    assertEquals(entityCount, 2);
    // custom op -> pig op, custom exec -> pig exec, custom op -> exec
    assertEquals(relationCount, 3);
  }

  private CustomOperationExecution prepExec(Source source) {
    CustomOperation op = new CustomOperation();
    op.setName("JobName");
    op.setPigOperationId("pigOperationId");
    op.setScript("LOAD data; DoStuff(data)");
    op.setNamespace("test");
    op.setSourceId(source.getIdentity());
    op.setOwner("owner");
    op.setIdentity(op.generateId());
    CustomOperationExecution exec = new CustomOperationExecution();
    exec.setPigExecutionId("pigExecId");
    exec.setCustomOperationId("instId");
    exec.setTemplate(op);
    exec.setNamespace(config.getNamespace());
    exec.setSourceId(source.getIdentity());
    return exec;
  }

}
