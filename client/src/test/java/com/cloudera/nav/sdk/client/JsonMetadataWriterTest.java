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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

import com.cloudera.nav.sdk.client.writer.JsonMetadataWriter;
import com.cloudera.nav.sdk.model.IdAttrs;
import com.cloudera.nav.sdk.model.Source;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.cloudera.nav.sdk.model.entities.HdfsEntity;
import com.cloudera.nav.sdk.model.entities.PigOperation;
import com.cloudera.nav.sdk.model.entities.PigOperationExecution;
import com.cloudera.nav.sdk.model.relations.DataFlowRelation;
import com.cloudera.nav.sdk.model.relations.Relation;
import com.cloudera.nav.sdk.model.relations.RelationIdGenerator;
import com.cloudera.nav.sdk.model.relations.RelationType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.Test;

public class JsonMetadataWriterTest {

  private ByteArrayOutputStream stream;
  private ClientConfig config;
  private HttpURLConnection mockConn;

  @Before
  public void setUp() throws IOException {
    stream = new ByteArrayOutputStream();
    mockConn = mock(HttpURLConnection.class);
    doReturn(200).when(mockConn).getResponseCode();
    config = mock(ClientConfig.class);
    doReturn("test").when(config).getNamespace();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWriteEntity() throws IOException {
    Source source = new Source("HDFS-1", SourceType.HDFS, "Cluster",
        "http://ns1", 0L);
    HdfsEntity entity = new HdfsEntity();
    entity.setSourceId(source.getIdentity());
    //entity.setFileSystemPath("/user/test");
    entity.setEntityType(EntityType.DIRECTORY);
    entity.setTags(ImmutableList.of("foo", "bar"));
    Map<String, String> props = Maps.newHashMap();
    props.put("foo", "bar");
    entity.setProperties(props);

    JsonMetadataWriter mWriter = new JsonMetadataWriter(config, stream,
        mockConn);
    mWriter.write(entity);

    String value = new String(stream.toByteArray());
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> values = ((List<Map<String, Object>>)mapper.readValue(value,
        Map.class).get("entities")).get(0);
    assertEquals(values.get("identity"), entity.getIdentity());
    assertEquals(values.get("internalType"), "fselement");
    //assertEquals(values.get("fileSystemPath"), entity.getFileSystemPath());
    assertEquals(values.get("sourceId"), source.getIdentity());
    assertEquals(values.get("sourceType"), SourceType.HDFS.name());
    assertEquals(values.get("type"), EntityType.DIRECTORY.name());
    assertEquals(values.get("deleted"), false);

    Map<String, Object> tChanges = (Map<String, Object>)values.get("tags");
    Collection<String> tags = (Collection<String>)tChanges.get("set");
    assertTrue(CollectionUtils.isEqualCollection(ImmutableList.of("foo", "bar"),
        tags));
    Map<String, Object> pDelta = (Map<String, Object>)values.get("properties");
    Map<String, String> udp = (Map<String, String>)pDelta.get("set");
    assertEquals(1, udp.size());
    assertEquals("bar", udp.get("foo"));
  }

  /**
   * Test add/del/set tags and UDP
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testChangeSets() throws IOException {
    Source source = new Source("HDFS-1", SourceType.HDFS, "Cluster",
        "http://ns1", 1L);
    HdfsEntity entity = new HdfsEntity("/user/test",
        EntityType.DIRECTORY, source.getIdentity());
    entity.setTags("foo", "bar");
    entity.removeTags("bar", "baz");
    entity.addTags("baz", "fizz");

    Map<String, String> props = Maps.newHashMap();
    props.put("foo", "A");
    props.put("bar", "B");
    entity.setProperties(props);
    entity.removeProperties(ImmutableSet.of("bar", "baz"));
    Map<String, String> newP = Maps.newHashMap();
    newP.put("baz", "C");
    newP.put("fizz", "D");
    entity.addProperties(newP);

    JsonMetadataWriter mWriter = new JsonMetadataWriter(config, stream,
        mockConn);
    mWriter.write(entity);

    String value = new String(stream.toByteArray());
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> values = ((List<Map<String, Object>>)mapper.readValue(value,
        Map.class).get("entities")).get(0);

    Map<String, Object> tChanges = (Map<String, Object>)values.get("tags");
    Collection<String> overrides = (Collection<String>)tChanges.get("set");
    Collection<String> add = (Collection<String>)tChanges.get("add");
    Collection<String> del = (Collection<String>)tChanges.get("del");
    assertEquals(Collections.singleton("foo"), Sets.newHashSet(overrides));
    assertEquals(Sets.newHashSet("baz", "fizz"), Sets.newHashSet(add));
    assertEquals(Collections.singleton("bar"), Sets.newHashSet(del));

    Map<String, Object> pChanges = (Map<String, Object>)values
        .get("properties");
    Map<String, String> overProps = (Map<String, String>)pChanges.get("set");
    Map<String, String> newProps = (Map<String, String>)pChanges.get("add");
    Collection<String> delProps = (Collection<String>)pChanges.get("del");
    assertTrue(overProps.size() == 1 && overProps.get("foo").equals("A"));
    assertTrue(newProps.size() == 2 && newProps.get("baz").equals("C")
        && newProps.get("fizz").equals("D"));
    assertEquals(Collections.singleton("bar"), Sets.newHashSet(delProps));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWriteRelation() throws IOException {
    Source source = new Source("HDFS-1", SourceType.HDFS, "Cluster",
        "http://ns1", 1L);
    HdfsEntity inputData = new HdfsEntity("/user/test/input",
        EntityType.DIRECTORY, source.getIdentity());
    inputData.setTags(ImmutableList.of("foo", "bar"));

    Collection<IdAttrs> ep1IdAttrsList = Lists.newArrayList();
    if (!inputData.getIsIdGenerated()) {
      IdAttrs at = new IdAttrs();
      inputData.populateIdAttrs(at);
      ep1IdAttrsList.add(at);
    }

    HdfsEntity outputData = new HdfsEntity("/user/test/output",
        EntityType.DIRECTORY, source.getIdentity());
    outputData.setTags(ImmutableList.of("foo", "bar"));

    Collection<IdAttrs> ep2IdAttrsList = Lists.newArrayList();
    if (!outputData.getIsIdGenerated()) {
      IdAttrs at = new IdAttrs();
      inputData.populateIdAttrs(at);
      ep2IdAttrsList.add(at);
    }

    Relation rel = DataFlowRelation.builder()
        .idGenerator(new RelationIdGenerator())
        .source(inputData)
        .target(outputData)
        .namespace("test")
        .userSpecified(true)
        .ep1Attributes(ep1IdAttrsList)
        .ep2Attributes(ep2IdAttrsList)
        .build();

    JsonMetadataWriter mWriter = new JsonMetadataWriter(config, stream,
        mockConn);
    mWriter.writeRelation(rel);

    String value = new String(stream.toByteArray());
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> values = ((List<Map<String, Object>>)mapper
        .readValue(value, Map.class).get("relations")).get(0);
    assertEquals(values.get("identity"), rel.getIdentity());
    assertEquals(values.get("namespace"), "test");
    assertEquals(values.get("type"), RelationType.DATA_FLOW.name());
    assertEquals(values.get("endpoint1SourceId"), source.getIdentity());
    assertEquals(values.get("endpoint2SourceId"), source.getIdentity());
    assertEquals(values.get("endpoint1SourceType"), SourceType.HDFS.name());
    assertEquals(values.get("endpoint2SourceType"), SourceType.HDFS.name());
    assertEquals(values.get("endpoint1Type"), EntityType.DIRECTORY.name());
    assertEquals(values.get("endpoint2Type"), EntityType.DIRECTORY.name());
    assertTrue(Boolean.valueOf(values.get("userSpecified").toString()));

    //TODO Need to assert the values of ep1Attributes and ep2Attributes
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWriteComposite() throws IOException {
    Source source = new Source("ExternalApp", SourceType.SDK, "ExternalApp",
        "http://appHost:port", 0L);
    CustomOperationExecution exec = prepExec(source);
    JsonMetadataWriter mWriter = new JsonMetadataWriter(config, stream,
        mockConn);
    mWriter.write(exec);

    ObjectMapper mapper = new ObjectMapper();
    String value = new String(stream.toByteArray());
    Map data = mapper.readValue(value, Map.class);
    Collection<?> entities = (Collection<?>) data.get("entities");
    Collection<?> relations = (Collection<?>) data.get("relations");

    // custom op and exec
    assertEquals(entities.size(), 2);
    // custom op -> pig op, custom exec -> pig exec, custom op -> exec
    assertEquals(relations.size(), 3);
  }

  private CustomOperationExecution prepExec(Source source) {
    CustomOperation op = new CustomOperation();
    PigOperation pigOperation = new PigOperation("LogicalPlanHash",
        "PigLatin");
    op.setName("JobName");
    op.setPigOperation(pigOperation);
    op.setScript("LOAD data; DoStuff(data)");
    op.setNamespace("test");
    op.setSourceId(source.getIdentity());
    op.setOwner("owner");
    op.setIdentity(op.generateId());
    CustomOperationExecution exec = new CustomOperationExecution();
    PigOperationExecution pigOperationExecution = new PigOperationExecution(
        "Script_ID", "PigLatin");
    exec.setPigExecution(pigOperationExecution);
    exec.setTemplate(op);
    exec.setNamespace(config.getNamespace());
    exec.setSourceId(source.getIdentity());
    return exec;
  }
}
