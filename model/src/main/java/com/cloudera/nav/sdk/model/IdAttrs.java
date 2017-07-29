// (c) Copyright 2015 Cloudera, Inc. All rights reserved.
package com.cloudera.nav.sdk.model;

import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.cloudera.nav.sdk.model.entities.TagChangeSet;
import com.cloudera.nav.sdk.model.entities.UDPChangeSet;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.joda.time.Instant;

/**
 * Used for deserializing custom entities. We use this as an intermediary
 * so that we can handle the changesets for tags/UDPs
 */
public class IdAttrs {

  @MProperty
  private String fileSystemPath;

  @MProperty
  private String jobName;

  @MProperty
  private String logicalPlanHash;

  @MProperty
  private String scriptId;

  public String getFileSystemPath() {
    return fileSystemPath;
  }

  public void setFileSystemPath(String fileSystemPath) {
    if (!Strings.isNullOrEmpty(fileSystemPath) &&
        fileSystemPath.endsWith("/")) {
      fileSystemPath = fileSystemPath.substring(0, fileSystemPath.length() - 1);
    }

    this.fileSystemPath = fileSystemPath;
  }

  public String getJobName() {
    return this.jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public String getLogicalPlanHash() {
    return this.logicalPlanHash;
  }

  public void setLogicalPlanHash(String logicalPlanHash) {
    this.logicalPlanHash = logicalPlanHash;
  }

  public void setScriptId(String scriptId) {
    this.scriptId = scriptId;
  }

  public String getScriptId() {
    return scriptId;
  }

  public void populateIdAttrs(IdAttrs idattrs) {
    idattrs.setFileSystemPath(this.fileSystemPath);
    idattrs.setLogicalPlanHash(this.logicalPlanHash);
    idattrs.setScriptId(this.scriptId);
    idattrs.setJobName(this.jobName);
  }
}
