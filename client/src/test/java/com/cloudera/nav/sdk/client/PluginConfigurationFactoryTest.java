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

import java.net.URL;

import org.junit.Test;

public class PluginConfigurationFactoryTest {

  @Test
  public void testCreateFromFile() {
    URL url = this.getClass().getClassLoader().getResource("nav_plugin.conf");
    ClientConfigFactory factory = new ClientConfigFactory();
    ClientConfig config = factory.readConfigurations(url.getPath());

    assertEquals(config.getApplicationUrl(), "http://external-app.com");
    assertEquals(config.getFormat(), Format.JSON);
    assertEquals(config.getMetadataParentUri().toString(),
        "http://nav.cloudera.com:7187/api/v7/plugin");
    assertEquals(config.getNamespace(), "tf");
    assertEquals(config.getNavigatorUrl(),
        "http://nav.cloudera.com:7187/api/v7/");
    assertEquals(config.getUsername(), "username");
    assertEquals(config.getPassword(), "password");
  }
}
