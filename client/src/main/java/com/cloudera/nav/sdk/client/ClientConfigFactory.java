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

import com.google.common.base.Throwables;

import java.net.URI;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Create PluginConfiguration instance
 */
public class ClientConfigFactory {

  // expected property names
  public static final String APP_URL = "application_url";
  public static final String FILE_FORMAT = "file_format";
  public static final String METADATA_URI = "metadata_parent_uri";
  public static final String NAMESPACE = "namespace";
  public static final String NAV_URL = "navigator_url";
  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";
  public static final String AUTOCOMMIT = "autocommit";
  public static final String DISABLE_SSL_VALIDATION = "disable_ssl_validation";
  public static final String SSL_KEYSTORE_LOCATION = "ssl_truststore_location";
  public static final String SSL_KEYSTORE_PASSWORD = "ssl_truststore_password";

  /**
   * Create a PluginConfiguration from the properties contained in the
   * given filePath
   *
   * @param filePath
   * @return
   */
  public ClientConfig readConfigurations(String filePath) {
    try {
      PropertiesConfiguration props = new PropertiesConfiguration(filePath);
      ClientConfig config = new ClientConfig();
      config.setApplicationUrl(props.getString(APP_URL));
      config.setFormat(Format.valueOf(
          props.getString(FILE_FORMAT, Format.JSON.name())));
      config.setMetadataParentUri(URI.create(props.getString(METADATA_URI)));
      config.setNamespace(props.getString(NAMESPACE));
      config.setNavigatorUrl(props.getString(NAV_URL));
      config.setUsername(props.getString(USERNAME));
      config.setPassword(props.getString(PASSWORD));
      config.setAutocommit(props.getBoolean(AUTOCOMMIT, false));
      config.setDisableSSLValidation(props.getBoolean(DISABLE_SSL_VALIDATION,
          false));
      config.setSSLTrustStoreLocation(props.getString(SSL_KEYSTORE_LOCATION,
          null));
      config.setSSLTrustStorePassword(props.getString(SSL_KEYSTORE_PASSWORD,
          null));
      return config;
    } catch (ConfigurationException e) {
      throw Throwables.propagate(e);
    }
  }

  public ClientConfig fromConfigMap(Map<String, Object> props) {
    ClientConfig config = new ClientConfig();
    config.setApplicationUrl(props.get(APP_URL).toString());
    Format format = props.containsKey(FILE_FORMAT) ?
        Format.valueOf(props.get(FILE_FORMAT).toString()) :
        Format.JSON;
    config.setFormat(format);
    config.setMetadataParentUri(URI.create(props.get(METADATA_URI).toString()));
    config.setNamespace(props.get(NAMESPACE).toString());
    config.setNavigatorUrl(props.get(NAV_URL).toString());
    config.setUsername(props.get(USERNAME).toString());
    config.setPassword(props.get(PASSWORD).toString());
    config.setAutocommit(props.containsKey(AUTOCOMMIT) ?
        Boolean.valueOf(props.get(AUTOCOMMIT).toString()) : false);
    config.setDisableSSLValidation(props.containsKey(DISABLE_SSL_VALIDATION) ?
        Boolean.valueOf(props.get(DISABLE_SSL_VALIDATION).toString()) : false);
    config.setSSLTrustStoreLocation(props.containsKey(SSL_KEYSTORE_LOCATION) ?
        props.get(SSL_KEYSTORE_LOCATION).toString() : null);
    config.setSSLTrustStorePassword(props.containsKey(SSL_KEYSTORE_PASSWORD) ?
        props.get(SSL_KEYSTORE_PASSWORD).toString() : null);
    return config;
  }
}
