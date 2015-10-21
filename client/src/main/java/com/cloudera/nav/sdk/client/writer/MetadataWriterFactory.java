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
package com.cloudera.nav.sdk.client.writer;

import com.cloudera.nav.sdk.client.ClientConfig;
import com.cloudera.nav.sdk.client.ClientUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;

import org.apache.commons.net.util.Base64;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A factory that returns the appropriate MetadataWriter given a set of
 * configurations
 */
public class MetadataWriterFactory {

  public static final String HDFS = "hdfs";
  public static final String LOCAL = "file";
  public static final String HTTP = "http";

  /**
   * Return lower cased metadata parent uri scheme. If null then empty
   * string is returned (for easier string comparisons).
   *
   * @param config
   * @return
   */
  @VisibleForTesting
  static String getScheme(ClientConfig config) {
    URI uri = config.getMetadataParentUri();
    String scheme = uri.getScheme();
    return scheme == null ? "" : scheme.toLowerCase();
  }

  /**
   * Create a new metadata writer based on the given configurations.
   * The configurations should determine the file format and the underlying
   * storage mechanism.
   *
   * @param config
   * @return
   */
  public MetadataWriter newWriter(ClientConfig config) {
    String scheme = getScheme(config);
    if (scheme.equals(HDFS) ) {
      throw new UnsupportedOperationException();
    } else if (scheme.equals(LOCAL)) {
      throw new UnsupportedOperationException();
    } else {
      try {
        HttpURLConnection conn = createHttpStream(config);
        OutputStream stream = new BufferedOutputStream(conn.getOutputStream());
        return new JsonMetadataWriter(config, stream, conn);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private HttpURLConnection createHttpStream(ClientConfig config)
      throws IOException {
    URL url = new URL(config.getMetadataParentUri().toASCIIString());
    HttpURLConnection conn = openConnection(url, config);
    conn.setRequestMethod("POST");
    String userpass = config.getUsername() + ":" + config.getPassword();
    String basicAuth = "Basic " + new String(Base64.encodeBase64(
        userpass.getBytes()));
    conn.addRequestProperty("Authorization", basicAuth);
    conn.addRequestProperty("Content-Type", "application/json");
    conn.setDoOutput(true);
    config.setProperty("conn", conn);
    return conn;
  }

  private HttpURLConnection openConnection(URL url, ClientConfig config)
      throws IOException {
    if (ClientUtils.isSSL(url.toString())) {
      HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
      conn.setHostnameVerifier(ClientUtils.getHostNameVerifier(config));
      conn.setSSLSocketFactory(ClientUtils.getSSLContext(config)
          .getSocketFactory());
      return conn;
    } else {
      return (HttpURLConnection) url.openConnection();
    }
  }

  private OutputStream createLocalFileStream(ClientConfig config) {
    String fileName = getFilePath(config.getMetadataParentUri().getPath());
    File file = new File(fileName);
    try {
      file.createNewFile();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    try {
      return new FileOutputStream(file);
    } catch (FileNotFoundException e) {
      throw Throwables.propagate(e);
    }
  }

  private OutputStream createHdfsStream(ClientConfig config) {
    try {
      FileSystem fs = FileSystem.get(config.getHadoopConfigurations());
      Path path = new Path(getFilePath(config.getMetadataParentUriString()));
      if (fs.exists(path)) {
        return fs.append(path);
      }
      // TODO block sizes, replication counts etc
      return fs.create(path);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private String getFilePath(String path) {
    // TODO file rotation
    if (!path.endsWith("/")) {
      path += "/";
    }
    return path + ".metadata";
  }

}
