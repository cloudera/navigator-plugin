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

package com.cloudera.nav.sdk.examples.tags;

import java.util.HashMap;
import java.util.Map;

import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.writer.ResultSet;
import com.cloudera.nav.sdk.model.Source;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.cloudera.nav.sdk.model.entities.HdfsEntity;
import com.google.common.collect.Sets;

/**
 * Tagging HDFS Files and Directories
 *
 * Tags is an important part of business metadata. This example uses the
 * Navigator plugin to tag an HDFS directory as sensitive.
 * Users and applications can then the tags to trigger actions such as
 * encryption, masking, and/or restrictions to permissions.
 */
public class SetHdfsFileTags {

  public static void main(String[] args) {

    // setup the plugin and api client
	  Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("navigator_url", "http://ip-192-168-100-9.ec2.internal:7187/api/v7/");
		configMap.put("username", "talend");
		configMap.put("password", "Cloudera!!");
		configMap.put("metadata_parent_uri", "http://ip-192-168-100-9.ec2.internal:7187/api/v7/metadata/plugin");
		configMap.put("autocommit", "true");
		configMap.put("application_url", "http://localhost");
		configMap.put("namespace", "talend");
		
    NavigatorPlugin plugin = NavigatorPlugin.fromConfigMap(configMap);
    NavApiCient client = plugin.getClient();
    Source fs = client.getOnlySource(SourceType.HDFS);

    // send tags for multiple entities to Navigator
    HdfsEntity dir = new HdfsEntity("/user/talend/dataoutput", EntityType.DIRECTORY,
        fs.getIdentity());
    dir.setTags(Sets.newHashSet("TALEND_TAG_5", "TALEND_TAG_6", "TALEND_TAG_7", "TALEND_TAG_8"));


    // Write metadata
    ResultSet results = plugin.write(dir);

    if (results.hasErrors()) {
      throw new RuntimeException(results.toString());
    }
  }
}