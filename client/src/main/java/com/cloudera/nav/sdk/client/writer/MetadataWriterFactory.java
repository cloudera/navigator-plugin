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
import com.cloudera.nav.sdk.client.SSLUtils;
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

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

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

  private final ClientConfig config;
  private final boolean isSSL;
  private final SSLContext sslContext;
  private final HostnameVerifier hostnameVerifier;

  public MetadataWriterFactory(ClientConfig config) {
    this.config = config;
    this.isSSL = SSLUtils.isSSL(config.getMetadataParentUriString());
    this.sslContext = isSSL ? SSLUtils.getSSLContext(config) : null;
    this.hostnameVerifier = isSSL ? SSLUtils.getHostnameVerifier(config) : null;
  }

  /**
   * Create a new metadata writer
   */
  public MetadataWriter newWriter() {
    String scheme = getScheme();
    if (scheme.equals(HDFS) ) {
      throw new UnsupportedOperationException();
    } else if (scheme.equals(LOCAL)) {
      throw new UnsupportedOperationException();
    } else {
      try {
        HttpURLConnection conn = createHttpStream();
        OutputStream stream = new BufferedOutputStream(conn.getOutputStream());
        return new JsonMetadataWriter(config, stream, conn);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Return lower cased metadata parent uri scheme. If null then empty
   * string is returned (for easier string comparisons).
   */
  private String getScheme() {
    URI uri = config.getMetadataParentUri();
    String scheme = uri.getScheme();
    return scheme == null ? "" : scheme.toLowerCase();
  }

  private HttpURLConnection createHttpStream()
      throws IOException {
    URL url = new URL(config.getMetadataParentUri().toASCIIString());
    HttpURLConnection conn = openConnection(url);
    conn.setRequestMethod("POST");
    String userpass = config.getUsername() + ":" + config.getPassword();
    String basicAuth = "Basic " + new String(Base64.encodeBase64(
        userpass.getBytes()));
    conn.addRequestProperty("Authorization", basicAuth);
    conn.addRequestProperty("Content-Type", "application/json");
    conn.setDoOutput(true);
    return conn;
  }

  private HttpURLConnection openConnection(URL url)
      throws IOException {
    if (isSSL) {
      HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
      conn.setHostnameVerifier(hostnameVerifier);
      conn.setSSLSocketFactory(sslContext.getSocketFactory());
      return conn;
    } else {
      return (HttpURLConnection) url.openConnection();
    }
  }

  private OutputStream createLocalFileStream() {
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

  private OutputStream createHdfsStream() {
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
