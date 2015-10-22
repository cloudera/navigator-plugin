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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.lang.StringUtils;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility functions to support communications with a TLS-enabled Navigator
 * server
 */
public class SSLUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SSLUtils.class);

  private static final String SSLCERTIFICATE =
      System.getProperty("java.vendor").contains("IBM") ? "ibmX509" : "SunX509";
  private static final String DEFAULT_TRUST_STORE_TYPE = "jks";

  /**
   * Whether the given urlString has TLS enabled
   * @param urlString
   */
  public static boolean isSSL(String urlString) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(urlString));
    return urlString.startsWith("https://");
  }

  /**
   * Return the TLS SSLContext with the TrustManager specified by the config
   * @param config
   */
  public static SSLContext getSSLContext(ClientConfig config) {
    try {
      SSLContext ctx = SSLContext.getInstance("TLS");
      ctx.init(null, new TrustManager[]{getTrustManager(config)}, null);
      return ctx;
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * If SSL validation is disabled (config.isDisableSSLValidation()), then
   * return a TrustManager that accepts everything.
   * Otherwise, return the override TrustManager in the config if specified,
   * or create a new TrustManager from the SSL configurations
   *
   * @param config
   */
  @VisibleForTesting
  static TrustManager getTrustManager(ClientConfig config) {
    if (config.isDisableSSLValidation()) {
      return new AcceptAllTrustManager();
    }
    if (config.getOverrideTrustManager() != null) {
      return config.getOverrideTrustManager();
    }
    return createTrustManager(config);
  }

  private static TrustManager createTrustManager(ClientConfig config) {
    Preconditions.checkNotNull(config.getSSLTrustStoreLocation(),
        "Could not create TrustManager, No SSL trust store provided");
    //trust store
    String trustStoreType = config.getSslTrustStoreType();
    if (StringUtils.isEmpty(trustStoreType)) {
      trustStoreType = DEFAULT_TRUST_STORE_TYPE;
    }
    String trustStoreLocation = config.getSSLTrustStoreLocation();
    Preconditions.checkArgument(StringUtils.isNotEmpty(trustStoreLocation),
      "Trust store location not provided");
    String trustStorePassword = config.getSSLTrustStorePassword();
    Preconditions.checkArgument(StringUtils.isNotEmpty(trustStorePassword),
        "Trust store password not provided");

    try {
      return loadTrustManager(trustStoreType,
          trustStoreLocation,
          trustStorePassword);
    } catch (IOException | GeneralSecurityException e) {
      throw Throwables.propagate(e);
    }
  }

  private static X509TrustManager loadTrustManager(String type,
                                                   String file,
                                                   String password)
      throws IOException, GeneralSecurityException {
    X509TrustManager trustManager = null;
    KeyStore ks = KeyStore.getInstance(type);
    try (FileInputStream in = new FileInputStream(file)) {
      ks.load(in, password.toCharArray());
      LOG.debug("Loaded truststore '" + file + "'");
    }

    TrustManagerFactory trustManagerFactory =
        TrustManagerFactory.getInstance(SSLCERTIFICATE);
    trustManagerFactory.init(ks);
    TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
    for (TrustManager trustManager1 : trustManagers) {
      if (trustManager1 instanceof X509TrustManager) {
        trustManager = (X509TrustManager) trustManager1;
        break;
      }
    }
    return trustManager;
  }

  /**
   * If SSL validation is disabled then return a HostnameVerifier that accepts
   * everything. Otherwise, return the override HostnameVerifier in the config
   * if specified, or return a new DefaultHostnameVerifier
   *
   * @param config
   */
  public static HostnameVerifier getHostnameVerifier(ClientConfig config) {
    if (config.isDisableSSLValidation()) {
      return new NoopHostnameVerifier();
    }
    if (config.getOverrideHostnameVerifier() == null) {
      return new DefaultHostnameVerifier();
    } else {
      return config.getOverrideHostnameVerifier();
    }
  }

  @VisibleForTesting
  static class AcceptAllTrustManager implements X509TrustManager {

    public void checkClientTrusted(X509Certificate[] xcs, String string)
        throws CertificateException {
      // You shall pass
    }

    public void checkServerTrusted(X509Certificate[] xcs, String string)
        throws CertificateException {
      // You shall pass
    }

    public X509Certificate[] getAcceptedIssuers() {
      return null;
    }
  }
}
