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
package com.cloudera.nav.plugin.model;

import com.google.common.base.Preconditions;

/**
 * Encapsulates information about Hadoop component services from which entities
 * and relations can be derived.
 */
public class Source {

  private final String name;
  private final SourceType sourceType;
  private final String clusterName;
  private final String sourceUrl;
  private final String identity;
  private final String sourceExtractIteration;

  /**
   * @param name
   * @param clusterName
   * @param sourceUrl
   * @param identity
   */
  public Source(String name, SourceType sourceType,
                String clusterName, String sourceUrl,
                String identity, String sourceExtractIteration) {
    Preconditions.checkNotNull(identity);
    this.name = name;
    this.sourceType = sourceType;
    this.clusterName = clusterName;
    this.sourceUrl = sourceUrl;
    this.identity = identity;
    this.sourceExtractIteration = sourceExtractIteration;
  }

  public Source(String name, SourceType sourceType,
                String clusterName, String sourceUrl, String sourceExtractIteration) {
    this(name, sourceType, clusterName, sourceUrl,
        SourceIdGenerator.generateSourceId(clusterName, name), sourceExtractIteration);
  }


  /**
   * @return the name of the Hadoop component service
   */
  public String getName() {
    return name;
  }

  /**
   * Source types are generally the canonical names of Hadoop comoponent
   * services like HDFS, HIVE, etc.
   *
   * @return the type of the metadata source
   */
  public SourceType getSourceType() {
    return sourceType;
  }

  /**
   * The name of the cluster assigned/set in Cloudera Manager
   *
   * @return the name of the cluster
   */
  public String getClusterName() {
    return clusterName;
  }

  /**
   * Source url's are generally the rpc addresses (host and port) of the
   * service
   *
   * @return the url for the Hadoop component service
   */
  public String getSourceUrl() {
    return sourceUrl;
  }

  /**
   * @return the id of this Source
   */
  public String getIdentity() {
    return identity;
  }

  public String getSourceExtractIteration(){ return sourceExtractIteration; }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Source source = (Source) o;

    return identity.equals(source.identity);
  }

  @Override
  public int hashCode() {
    return identity.hashCode();
  }


}
