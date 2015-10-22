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

import java.io.File;
import java.security.cert.X509Certificate;
import java.util.Map;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.io.FileUtils;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.runners.*;

@RunWith(MockitoJUnitRunner.class)
public class SSLUtilsTest {

  private static final String BASEDIR =
      System.getProperty("test.build.dir", "target/test-dir") + "/" +
          SSLUtilsTest.class.getSimpleName();
  private static Map<String, X509Certificate> certs;

  private ClientConfig config;

  @Before
  public void setUp() {
    Map<String, Object> confMap = Maps.newHashMap();
    confMap.put(ClientConfigFactory.APP_URL, "localhost");
    confMap.put(ClientConfigFactory.METADATA_URI, "localhost");
    confMap.put(ClientConfigFactory.NAV_URL, "localhost");
    confMap.put(ClientConfigFactory.NAMESPACE, "test");
    confMap.put(ClientConfigFactory.USERNAME, "user");
    confMap.put(ClientConfigFactory.PASSWORD, "pass");
    config = (new ClientConfigFactory()).fromConfigMap(confMap);
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    File base = new File(BASEDIR);
    FileUtils.deleteDirectory(base);
    base.mkdirs();
    certs = SSLTestUtils.setupSSLConfig(BASEDIR);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    SSLTestUtils.cleanupSSLConfig(BASEDIR);
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
    config.setSSLTrustStoreLocation(BASEDIR + "/trust.jks");
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
