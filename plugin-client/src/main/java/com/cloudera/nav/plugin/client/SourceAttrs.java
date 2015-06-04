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
package com.cloudera.nav.plugin.client;

import com.cloudera.nav.plugin.model.Source;
import com.cloudera.nav.plugin.model.SourceType;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Used by NavApiClient for JSON deserialization because Source is
 * immutable
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class SourceAttrs {

  private String clusterName;
  private String originalName;
  private SourceType sourceType;
  private String sourceUrl;
  private String identity;
  private String sourceExtractIteration;

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public String getOriginalName() {
    return originalName;
  }

  public void setOriginalName(String originalName) {
    this.originalName = originalName;
  }

  public String getSourceUrl() {
    return sourceUrl;
  }

  public void setSourceUrl(String sourceUrl) {
    this.sourceUrl = sourceUrl;
  }

  public String getIdentity() {
    return identity;
  }

  public void setIdentity(String identity) {
    this.identity = identity;
  }

  public SourceType getSourceType() {
    return sourceType;
  }

  public void setSourceType(SourceType sourceType) {
    this.sourceType = sourceType;
  }

  public String getSourceExtractIteration() {
    return sourceExtractIteration;
  }

  public void setSourceExtractIteration(String sourceExtractIteration) { this.sourceExtractIteration = sourceExtractIteration; }

  public Source createSource() {
    return new Source(getOriginalName(), getSourceType(),
        getClusterName(), getSourceUrl(), getIdentity(), getSourceExtractIteration());
  }
}
