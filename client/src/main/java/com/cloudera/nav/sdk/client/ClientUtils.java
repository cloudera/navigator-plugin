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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collection;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.lang.StringUtils;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

/**
 * A set of functions for forming and handling URLs and  Solr queries.
 */
public class ClientUtils {

  /**
   * Joining two URL components properly by handling slashes
   *
   * @param base First part of the URL
   * @param component Second part of the URL
   * @return String url of base/component
   */
  public static String joinUrlPath(String base, String component) {
    return base + (base.endsWith("/") ? "" : "/") + component;
  }

  /**
   * Makes a conjunctive "AND" Solr query with two clauses.
   *
   * @param q1 Solr query string
   * @param q2 Solr query string
   * @return (q1) AND (q2)
   */
  public static String conjoinSolrQueries(String q1, String q2){
    if(StringUtils.isEmpty(q1)){
      return q2;
    }
    if(StringUtils.isEmpty(q2)){
      return q1;
    }
    return q1 + " AND " + q2 ;
  }

  /**
   * Constructs a single conjunctive clause of values with "OR"
   *
   * @param fieldName field name that clause values can satisfy
   * @param values iterable of possible values
   * @return conjunctive clause string as fieldName:(a OR b ...)
   */
  public static String buildConjunctiveClause(String fieldName,
                                              Collection<String> values){
    Joiner conjunctiveJoiner = Joiner.on(" OR ").skipNulls();
    return fieldName + ":(" + conjunctiveJoiner.join(values) + ")";
  }

  /**
   * Convert desired values to valid query string in solr syntax in
   * conjunctive normal form as sourceType:(i OR j ...) AND type:(a OR b ...)
   *
   * @param sourceTypes collection of sourceType field values
   * @param types collection of type field values
   * @return Conjunctive clauses of sourceTypes and types
   */
  public static String buildQuery(Collection<String> sourceTypes,
                                  Collection<String> types){
    String sourceClause = buildConjunctiveClause("sourceType", sourceTypes);
    String typeClause = buildConjunctiveClause("type", types);
    return conjoinSolrQueries(sourceClause, typeClause);
  }

  public static TrustManager getTrustManager(ClientConfig conf) {
    if (conf.isDisableSSLValidation()) {
      return new AcceptAllTrustManager();
    }
    // TODO a real one
    return null;
  }

  public static boolean isSSL(String urlString) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(urlString));
    return urlString.startsWith("https://");
  }

  public static SSLContext getSSLContext(ClientConfig config) {
    try {
      SSLContext ctx = SSLContext.getInstance("TLS");
      ctx.init(null, new TrustManager[]{getTrustManager(config)}, null);
      return ctx;
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      throw Throwables.propagate(e);
    }
  }

  public static RestTemplate newRestTemplate(ClientConfig config) {
    if (ClientUtils.isSSL(config.getNavigatorUrl())) {
      CloseableHttpClient httpClient = HttpClients.custom()
          .setSSLContext(ClientUtils.getSSLContext(config))
          .setSSLHostnameVerifier(getHostNameVerifier(config))
          .build();
      HttpComponentsClientHttpRequestFactory requestFactory =
          new HttpComponentsClientHttpRequestFactory();
      requestFactory.setHttpClient(httpClient);
      return new RestTemplate(requestFactory);
    } else {
      return new RestTemplate();
    }
  }

  public static HostnameVerifier getHostNameVerifier(ClientConfig config) {
    if (config.isDisableSSLValidation()) {
      return new NoopHostnameVerifier();
    }
    // TODO a real one
    return null;
  }

  private static class AcceptAllTrustManager implements X509TrustManager {

    public void checkClientTrusted(X509Certificate[] xcs, String string)
        throws CertificateException {
      // no op
    }

    public void checkServerTrusted(X509Certificate[] xcs, String string)
        throws CertificateException {
      // no op
    }

    public X509Certificate[] getAcceptedIssuers() {
      return null;
    }
  }
}
