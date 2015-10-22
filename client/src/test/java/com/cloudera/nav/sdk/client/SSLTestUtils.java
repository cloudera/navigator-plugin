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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Map;

import sun.security.x509.AlgorithmId;
import sun.security.x509.CertificateAlgorithmId;
import sun.security.x509.CertificateIssuerName;
import sun.security.x509.CertificateSerialNumber;
import sun.security.x509.CertificateSubjectName;
import sun.security.x509.CertificateValidity;
import sun.security.x509.CertificateVersion;
import sun.security.x509.CertificateX509Key;
import sun.security.x509.X500Name;
import sun.security.x509.X509CertImpl;
import sun.security.x509.X509CertInfo;

/**
 * Utilities for testing the SSLUtils class
 */
public class SSLTestUtils {

  /**
   * Performs complete setup of SSL configuration in preparation for testing an
   * SSLFactory.  This includes keys, certs, keystores, truststores, the server
   * SSL configuration file, the client SSL configuration file, and the master
   * configuration file read by the SSLFactory.
   *
   * @param keystoresDir String directory to save keystores
   */
  public static Map<String, X509Certificate> setupSSLConfig(String keystoresDir)
      throws Exception {
    String clientKS = keystoresDir + "/client.jks";
    String clientPassword = "clientP";
    String trustKS = keystoresDir + "/trust.jks";
    String trustPassword = "trustP";

    Map<String, X509Certificate> certs = Maps.newHashMap();

    KeyPair cKP = generateKeyPair("RSA");
    X509Certificate cCert = generateCertificate("CN=localhost, O=client", cKP,
        30, "SHA1withRSA");
    createKeyStore(clientKS, clientPassword, "client",
        cKP.getPrivate(), cCert);
    certs.put("client", cCert);

    createTrustStore(trustKS, trustPassword, certs);
    return certs;
  }

  public static void cleanupSSLConfig(String keystoresDir) throws Exception {
    File f = new File(keystoresDir + "/clientKS.jks");
    f.delete();
    f = new File(keystoresDir + "/trustKS.jks");
    f.delete();
  }

  private static KeyPair generateKeyPair(String algorithm)
      throws NoSuchAlgorithmException {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance(algorithm);
    keyGen.initialize(1024);
    return keyGen.genKeyPair();
  }
  /**
   * Create a self-signed X.509 Certificate.
   * From http://bfo.com/blog/2011/03/08/odds_and_ends_creating_a_new_x_509_certificate.html.
   *
   * @param dn the X.509 Distinguished Name, eg "CN=Test, L=London, C=GB"
   * @param pair the KeyPair
   * @param days how many days from now the Certificate is valid for
   * @param algorithm the signing algorithm, eg "SHA1withRSA"
   * @return the self-signed certificate
   * @throws IOException thrown if an IO error ocurred.
   * @throws GeneralSecurityException thrown if an Security error ocurred.
   */
  public static X509Certificate generateCertificate(String dn, KeyPair pair,
                                                    int days, String algorithm)
      throws GeneralSecurityException, IOException {
    PrivateKey privkey = pair.getPrivate();
    X509CertInfo info = new X509CertInfo();
    Date from = new Date();
    Date to = new Date(from.getTime() + days * 86400000l);
    CertificateValidity interval = new CertificateValidity(from, to);
    BigInteger sn = new BigInteger(64, new SecureRandom());
    X500Name owner = new X500Name(dn);

    String javaVersion = System.getProperty("java.version");
    if (javaVersion.startsWith("1.8")) {
      info.set(X509CertInfo.SUBJECT, owner);
      info.set(X509CertInfo.ISSUER, owner);
    } else {
      info.set(X509CertInfo.SUBJECT, new CertificateSubjectName(owner));
      info.set(X509CertInfo.ISSUER, new CertificateIssuerName(owner));
    }

    info.set(X509CertInfo.VALIDITY, interval);
    info.set(X509CertInfo.SERIAL_NUMBER, new CertificateSerialNumber(sn));
    info.set(X509CertInfo.KEY, new CertificateX509Key(pair.getPublic()));
    info
        .set(X509CertInfo.VERSION, new CertificateVersion(CertificateVersion.V3));
    AlgorithmId algo = new AlgorithmId(AlgorithmId.md5WithRSAEncryption_oid);
    info.set(X509CertInfo.ALGORITHM_ID, new CertificateAlgorithmId(algo));

    // Sign the cert to identify the algorithm that's used.
    X509CertImpl cert = new X509CertImpl(info);
    cert.sign(privkey, algorithm);

    // Update the algorith, and resign.
    algo = (AlgorithmId) cert.get(X509CertImpl.SIG_ALG);
    info
        .set(CertificateAlgorithmId.NAME + "." + CertificateAlgorithmId.ALGORITHM,
            algo);
    cert = new X509CertImpl(info);
    cert.sign(privkey, algorithm);
    return cert;
  }

  private static void createKeyStore(String filename,
                                     String password, String alias,
                                     Key privateKey, Certificate cert)
      throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    ks.setKeyEntry(alias, privateKey, password.toCharArray(),
        new Certificate[]{cert});
    saveKeyStore(ks, filename, password);
  }

  private static KeyStore createEmptyKeyStore()
      throws GeneralSecurityException, IOException {
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(null, null); // initialize
    return ks;
  }

  private static void saveKeyStore(KeyStore ks, String filename,
                                   String password)
      throws GeneralSecurityException, IOException {
    FileOutputStream out = new FileOutputStream(filename);
    try {
      ks.store(out, password.toCharArray());
    } finally {
      out.close();
    }
  }

  private static <T extends Certificate> void createTrustStore(
      String filename, String password, Map<String, T> certs)
      throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    for (Map.Entry<String, T> cert : certs.entrySet()) {
      ks.setCertificateEntry(cert.getKey(), cert.getValue());
    }
    saveKeyStore(ks, filename, password);
  }
}