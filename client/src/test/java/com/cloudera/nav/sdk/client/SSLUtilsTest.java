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

import static org.junit.Assert.*;

import com.google.common.collect.Maps;

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.Map;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.runners.*;

@RunWith(MockitoJUnitRunner.class)
public class SSLUtilsTest {

  private static Map<String, Certificate> certs;

  private ClientConfig config;

  @Before
  public void setUp() throws Exception {
    Map<String, Object> confMap = Maps.newHashMap();
    confMap.put(ClientConfigFactory.APP_URL, "localhost");
    confMap.put(ClientConfigFactory.METADATA_URI, "localhost");
    confMap.put(ClientConfigFactory.NAV_URL, "localhost");
    confMap.put(ClientConfigFactory.NAMESPACE, "test");
    confMap.put(ClientConfigFactory.USERNAME, "user");
    confMap.put(ClientConfigFactory.PASSWORD, "pass");
    config = (new ClientConfigFactory()).fromConfigMap(confMap);

    KeyStore keyStore = KeyStore.getInstance("jks");
    ClassLoader classLoader = getClass().getClassLoader();
    String keyStoreLocation = classLoader.getResource("client.jks").getFile();
    try (InputStream is = new FileInputStream(keyStoreLocation)) {
      keyStore.load(is, "clientP".toCharArray());
    }
    certs = Maps.newHashMap();
    Enumeration<String> aliasesEn = keyStore.aliases();
    String alias;
    while(aliasesEn.hasMoreElements()) {
      alias = aliasesEn.nextElement();
      certs.put(alias, keyStore.getCertificate(alias));
    }
  }

  @Test
  public void testIsSSL() {
    assertTrue(SSLUtils.isSSL("https://localhost:7187"));
    assertFalse(SSLUtils.isSSL("http://localhost:7187"));
  }

  @Test
  public void testGetHostnameVerifier() {
    // Default
    HostnameVerifier verifier = SSLUtils.getHostnameVerifier(config);
    assertTrue(verifier instanceof DefaultHostnameVerifier);

    // Override
    config.setOverrideHostnameVerifier(new TestHostnameVerifier());
    verifier = SSLUtils.getHostnameVerifier(config);
    assertTrue(verifier instanceof TestHostnameVerifier);

    // Disabled
    config.setDisableSSLValidation(true);
    verifier = SSLUtils.getHostnameVerifier(config);
    assertTrue(verifier instanceof NoopHostnameVerifier);
  }

  @Test
  public void testGetTrustManager() throws Exception {
    // From config
    ClassLoader classLoader = getClass().getClassLoader();
    config.setSSLTrustStoreLocation(classLoader.getResource("trust.jks")
        .getFile());
    config.setSSLTrustStorePassword("trustP");
    TrustManager trustManager = SSLUtils.getTrustManager(config);
    assertTrue(trustManager instanceof X509TrustManager);
    ((X509TrustManager)trustManager).checkClientTrusted(
        certs.values().toArray(new X509Certificate[certs.size()]), "RSA");

    // Override
    config.setOverrideTrustManager(new TestTrustManager());
    trustManager = SSLUtils.getTrustManager(config);
    assertTrue(trustManager instanceof TestTrustManager);

    // Disabled
    config.setDisableSSLValidation(true);
    trustManager = SSLUtils.getTrustManager(config);
    assertTrue(trustManager instanceof SSLUtils.AcceptAllTrustManager);
  }

  private class TestHostnameVerifier implements HostnameVerifier {
    @Override
    public boolean verify(String s, SSLSession sslSession) {
      return false;
    }
  }

  private class TestTrustManager implements TrustManager {
  }
}
