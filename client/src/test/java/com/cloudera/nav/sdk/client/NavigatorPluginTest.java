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
import static org.mockito.Mockito.*;

import com.cloudera.nav.sdk.client.writer.MetadataWriter;
import com.cloudera.nav.sdk.client.writer.MetadataWriterFactory;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.entities.HdfsEntity;
import com.google.common.collect.Iterables;

import java.net.URL;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.runners.*;

@RunWith(MockitoJUnitRunner.class)
public class NavigatorPluginTest {

  private MetadataWriter mockWriter;
  private MetadataWriterFactory mockFactory;
  @Captor
  private ArgumentCaptor<Collection<Entity>> captor;

  @Before
  public void setUp() {
    mockWriter = mock(MetadataWriter.class);
    mockFactory = mock(MetadataWriterFactory.class);
    doReturn(mockWriter).when(mockFactory).newWriter(
        any(ClientConfig.class));
  }

  @Test
  public void testWrite() {
    URL url = this.getClass().getClassLoader().getResource("nav_plugin.conf");
    ClientConfigFactory factory = new ClientConfigFactory();
    ClientConfig config = factory.readConfigurations(url.getPath());
    NavigatorPlugin plugin = new NavigatorPlugin(config, mockFactory);
    HdfsEntity entity = new HdfsEntity();
    entity.setIdentity("foo");
    plugin.write(entity);
    verify(mockWriter).write(captor.capture());
    assertEquals(Iterables.getOnlyElement(captor.getValue()), entity);
    verify(mockWriter).flush();
    verify(mockWriter).close();
  }
}
