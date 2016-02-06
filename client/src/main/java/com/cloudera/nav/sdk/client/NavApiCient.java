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

import com.cloudera.nav.sdk.model.Source;
import com.cloudera.nav.sdk.model.SourceType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

/**
 * An API client to communicate with Navigator to register and validate
 * metadata models
 */
public class NavApiCient {

  private static final Logger LOG = LoggerFactory.getLogger(NavApiCient.class);
  private static final String SOURCE_QUERY = "type:SOURCE";

  private final ClientConfig config;
  private final Cache<String, Source> sourceCacheByUrl;
  private final Cache<SourceType, Collection<Source>> sourceCacheByType;
  private final boolean isSSL;
  private final SSLContext sslContext;
  private final HostnameVerifier hostnameVerifier;

  public NavApiCient(ClientConfig config) {
    this.config = config;
    this.sourceCacheByUrl = CacheBuilder.newBuilder().build();
    this.sourceCacheByType = CacheBuilder.newBuilder().build();
    this.isSSL = SSLUtils.isSSL(config.getNavigatorUrl());
    this.sslContext = isSSL ? SSLUtils.getSSLContext(config) : null;
    this.hostnameVerifier = isSSL ? SSLUtils.getHostnameVerifier(config) : null;
  }

  /**
   * Registers a given set of metadata models
   *
   * @param models
   */
  public void registerModels(Collection<Object> models) {
    throw new UnsupportedOperationException("not yet implemented");
  }

  /**
   * Call the Navigator API and retrieve all available sources
   *
   * @return a collection of available sources
   */
  public Collection<Source> getAllSources() {
    RestTemplate restTemplate = newRestTemplate();
    String url = getUrl();
    HttpHeaders headers = getAuthHeaders();
    HttpEntity<String> request = new HttpEntity<String>(headers);
    ResponseEntity<SourceAttrs[]> response = restTemplate.exchange(url,
        HttpMethod.GET, request, SourceAttrs[].class);
    Collection<Source> sources = Lists.newArrayList();
    for (SourceAttrs info : response.getBody()) {
      sources.add(info.createSource());
    }
    return sources;
  }

  /**
   * Constructs relation API call from query, and cursorMark.Returns a batch of
   * results that satisfy the query, starting from the cursorMark.
   * Called in next() of IncrementalExtractIterator()
   *
   * @param metadataQuery Solr query string, cursormark and limit
   * @return ResultsBatch set of results that satisfy query and next cursor
   */
  public ResultsBatch<Map<String, Object>> getRelationBatch(
      MetadataQuery metadataQuery) {
    String fullUrlPost = getUrl("relations");
    return queryNav(fullUrlPost, metadataQuery, RelationResultsBatch.class);
  }

  /**
   * {@link #getRelationBatch(MetadataQuery) getRelationBatch} with entities
   */
  public ResultsBatch<Map<String, Object>> getEntityBatch(
      MetadataQuery metadataQuery) {
    String fullUrlPost = getUrl("entities");
    return queryNav(fullUrlPost, metadataQuery, EntityResultsBatch.class);
  }

  /**
   * Constructs a POST Request from the given URL and body and returns the
   * response body contains a batch of results.
   *
   * @param url           URl being posted to
   * @param metadataQuery query criteria for metadata being retrieved to satisfy
   * @param resultClass   type of ResultsBatch to be returned
   * @return ResultsBatch of entities or relations that specify the
   * query parameters in the URL and request body
   */
  @VisibleForTesting
  public ResultsBatch<Map<String, Object>> queryNav(String url,
                                                    MetadataQuery metadataQuery,
                                                    Class<? extends ResultsBatch<Map<String, Object>>> resultClass) {
    RestTemplate restTemplate = newRestTemplate();
    HttpHeaders headers = getAuthHeaders();
    HttpEntity<MetadataQuery> request =
        new HttpEntity<MetadataQuery>(metadataQuery, headers);
    return restTemplate.exchange(url, HttpMethod.POST, request,
        resultClass).getBody();
  }

  private RestTemplate newRestTemplate() {
    if (isSSL) {
      CloseableHttpClient httpClient = HttpClients.custom()
          .setSSLContext(sslContext)
          .setSSLHostnameVerifier(hostnameVerifier)
          .build();
      HttpComponentsClientHttpRequestFactory requestFactory =
          new HttpComponentsClientHttpRequestFactory();
      requestFactory.setHttpClient(httpClient);
      return new RestTemplate(requestFactory);
    } else {
      return new RestTemplate();
    }
  }

  public ClientConfig getConfig() {
    return config;
  }

  /**
   * Get the Source corresponding to the Hadoop service Url from Navigator.nvmd
   * A NoSuchElementException is thrown if the url does not correspond to
   * any known Source
   *
   * @param serviceUrl
   * @return
   */
  public Source getSourceForUrl(String serviceUrl) {
    Source source = sourceCacheByUrl.getIfPresent(serviceUrl);
    if (source == null) {
      loadAllSources();
    }
    source = sourceCacheByUrl.getIfPresent(serviceUrl);
    Preconditions.checkArgument(source != null,
        "Could not find Source at " + serviceUrl);
    return source;
  }

  /**
   * Return the only Source of the given type, throw exception if more than
   * one is found.
   *
   * @param sourceType
   * @return
   */
  public Source getOnlySource(SourceType sourceType) {
    Collection<Source> sources = getSourcesForType(sourceType);
    Preconditions.checkNotNull(sources, "Could not find sources for " +
        "source type " + sourceType.name());
    return Iterables.getOnlyElement(sources);
  }

  public Collection<Source> getSourcesForType(SourceType sourceType) {
    Collection<Source> sources = sourceCacheByType.getIfPresent(sourceType);
    if (sources == null) {
      loadAllSources();
      sources = sourceCacheByType.getIfPresent(sourceType);
    }
    return sources;
  }

  /**
   * Clear the cache of Sources that have been previously loaded.
   */
  public void resetSources() {
    sourceCacheByUrl.invalidateAll();
    sourceCacheByType.invalidateAll();
  }

  /**
   * Form headers for sending API calls to the Navigator server
   *
   * @return HttpHeaders headers for authorizing the plugin
   */
  private HttpHeaders getAuthHeaders() {
    // basic authentication with base64 encoding
    String plainCreds = String.format("%s:%s", config.getUsername(),
        config.getPassword());
    byte[] plainCredsBytes = plainCreds.getBytes();
    byte[] base64CredsBytes = Base64.encodeBase64(plainCredsBytes);
    String base64Creds = new String(base64CredsBytes);
    HttpHeaders headers = new HttpHeaders();
    headers.add("Authorization", "Basic " + base64Creds);
    return headers;
  }

  /**
   * @return url for querying all sources
   */
  private String getUrl() {
    // form the url string to request all entities with type equal to SOURCE
    String baseNavigatorUrl = config.getNavigatorUrl();
    String entitiesUrl = joinUrlPath(baseNavigatorUrl, "entities");
    return String.format("%s?query=%s", entitiesUrl, SOURCE_QUERY);
  }

  private String getUrl(String type) {
    String baseNavigatorUrl = config.getNavigatorUrl();
    String typeUrl = joinUrlPath(baseNavigatorUrl, type);
    return typeUrl + "/paging";
  }

  private void loadAllSources() {
    Collection<Source> allSources = getAllSources();
    for (Source source : allSources) {
      if (source.getSourceUrl() == null) {
        LOG.warn(String.format("Source %s did not have a source url",
            source.getName() != null ? source.getName() :
                source.getIdentity()));
        continue;
      }
      sourceCacheByUrl.put(source.getSourceUrl(), source);
      try {
        Collection<Source> forType = sourceCacheByType.get(
            source.getSourceType(),
            new Callable<Collection<Source>>() {
              @Override
              public Collection<Source> call() throws Exception {
                return Sets.newHashSet();
              }
            });
        forType.add(source);
      } catch (ExecutionException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private static String joinUrlPath(String base, String component) {
    return base + (base.endsWith("/") ? "" : "/") + component;
  }
}
