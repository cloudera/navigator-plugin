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

import com.cloudera.nav.sdk.client.writer.MetadataWriter;
import com.cloudera.nav.sdk.client.writer.MetadataWriterFactory;
import com.cloudera.nav.sdk.client.writer.ResultSet;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

/**
 * Communicates with Navigator to register custom entity models and
 * write enriched data to be combined with Navigator's lineage and metadata
 * information
 */
public class NavigatorPlugin {

  /**
   * Use the information contained in the given configuration file to
   * create a new NavigatorPlugin
   *
   * @param configFilePath local file system path of plugin configurations
   * @return a new NavigatorPlugin instance
   */
  public static NavigatorPlugin fromConfigFile(String configFilePath) {
    ClientConfig config = (new ClientConfigFactory())
        .readConfigurations(configFilePath);
    return new NavigatorPlugin(config);
  }

  public static NavigatorPlugin fromConfigMap(Map<String, Object> configMap) {
    ClientConfig config = (new ClientConfigFactory())
        .fromConfigMap(configMap);
    return new NavigatorPlugin(config);
  }

  private final ClientConfig config;
  private final MetadataWriterFactory factory;
  private final NavApiCient client;

  /**
   * The plugin must be configured with the URL for the Navigator API and the
   * location for saving new metadata information
   *
   * @param config
   * @param factory
   */
  public NavigatorPlugin(ClientConfig config,
                         MetadataWriterFactory factory) {
    Preconditions.checkArgument(!StringUtils.isEmpty(config.getNavigatorUrl()));
    Preconditions.checkArgument(config.getMetadataParentUri() != null);
    Preconditions.checkNotNull(factory);
    this.config = config;
    this.factory = factory;
    this.client = new NavApiCient(config);
  }

  /**
   * The plugin must be configured with the URL for the Navigator API and the
   * location for saving new metadata information
   *
   * @param config
   */
  public NavigatorPlugin(ClientConfig config) {
    this(config, new MetadataWriterFactory(config));
  }

  /**
   * Currently this is unsupported. Instead, a generic custom entity is
   * being used by Navigator.
   *
   * Search for classes defined using the @MClass annotation
   * in the given package. Registers all found classes with Navigator
   * @param packageName
   */
  public void registerModels(String packageName) {
    throw new UnsupportedOperationException();
  }

  /**
   * Currently this is unsupported. Instead, a generic custom entity is
   * being used by Navigator.
   *
   * Register a single class with Navigator. It must have the @MClass
   * annotation
   * @param entityClass
   */
  public void registerModel(Class<? extends Entity> entityClass) {
    throw new UnsupportedOperationException();
  }

  /**
   * Write the custom entity
   * @param entity
   */
  public ResultSet write(Entity entity) {
    return write(ImmutableList.of(entity));
  }

  /**
   * Write a collection of custom entities
   * @param entities
   */
  public ResultSet write(Collection<Entity> entities) {
    MetadataWriter writer = factory.newWriter();
    try {
      writer.write(entities);
      writer.flush();
    } finally {
      writer.close();
    }
    return writer.getLastResultSet();
  }

  /**
   * @return a client to communicate with the Navigator REST API
   */
  public NavApiCient getClient() {
    return client;
  }

  public ClientConfig getConfig() {
    return config;
  }

  public String getNamespace() {
    return config.getNamespace();
  }
}
