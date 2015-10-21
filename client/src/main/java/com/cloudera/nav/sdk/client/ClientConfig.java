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

import com.google.common.collect.Maps;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * A set of configuration options needed by the Navigator plugin
 */
public class ClientConfig {

  private String navigatorUrl;
  private URI metadataParentUri;
  private String applicationUrl;
  private String namespace;
  private String username;
  private String password;
  private Configuration hadoopConf;
  private Format format;
  private Map<String, Object> props;
  private boolean autocommit;
  private boolean disableSSLValidation;

  public ClientConfig() {
    props = Maps.newHashMap();
  }

  /**
   * @return Location of the Navigator API server
   */
  public String getNavigatorUrl() {
    return navigatorUrl;
  }

  /**
   * Sets the URL for the Navigator API server
   * @param navigatorUrl new URL for Navigator API server
   */
  public void setNavigatorUrl(String navigatorUrl) {
    this.navigatorUrl = navigatorUrl;
  }

  /**
   * @return URI for the parent under which metadata will be written
   */
  public URI getMetadataParentUri() {
    return metadataParentUri;
  }

  /**
   * @return String representation of the metadata parent URI
   */
  public String getMetadataParentUriString() {
    return metadataParentUri.toString();
  }

  /**
   * Change the metadata parent location. An URISyntaxException is
   * thrown if the given String is an invalid URI.
   * @param metadataParent
   */
  public void setMetadataParentUri(String metadataParent)
      throws URISyntaxException {
    // for local file paths
    if (metadataParent.startsWith("/")) {
      metadataParent = "file://" + metadataParent;
    }
    setMetadataParentUri(new URI(metadataParent));
  }

  /**
   * Change the metadata parent location
   * @param metadataParent
   */
  public void setMetadataParentUri(URI metadataParent) {
    this.metadataParentUri = metadataParent;
  }

  /**
   * @return application namespace assigned by Navigator
   */
  public String getNamespace() {
    return namespace;
  }

  /**
   * Set the application namespace assigned by Navigator
   * @param namespace
   */
  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  /**
   * Navigator username
   * @return
   */
  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  /**
   * Navigator password
   * @return
   */
  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  /**
   * @return the client application URL (used to record the source of
   *         custom entities
   */
  public String getApplicationUrl() {
    return applicationUrl;
  }

  public void setApplicationUrl(String applicationUrl) {
    this.applicationUrl = applicationUrl;
  }

  /**
   * @return the hadoop configurations used to create a connection to HDFS
   */
  public Configuration getHadoopConfigurations() {
    return hadoopConf;
  }

  public void setHadoopConfigurations(Configuration conf) {
    this.hadoopConf = conf;
  }

  /**
   * @return the file format used to read and write
   */
  public Format getFormat() {
    return format;
  }

  public void setFormat(Format format) {
    this.format = format;
  }

  public void setProperty(String key, Object value) {
    props.put(key, value);
  }

  public Map<String, Object> getProperties() {
    return props;
  }

  public Object getProperty(String key) {
    return props.get(key);
  }

  public boolean isAutocommit() {
    return autocommit;
  }

  public void setAutocommit(boolean autocommit) {
    this.autocommit = autocommit;
  }

  public void setDisableSSLValidation(boolean disableSSLValidation) {
    this.disableSSLValidation = disableSSLValidation;
  }

  /**
   * Whether to skip SSL certificate Validation altogether
   */
  public boolean isDisableSSLValidation() {
    return disableSSLValidation;
  }
}
