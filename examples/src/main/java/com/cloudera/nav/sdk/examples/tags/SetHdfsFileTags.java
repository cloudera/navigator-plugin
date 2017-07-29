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

import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.writer.ResultSet;
import com.cloudera.nav.sdk.model.IdAttrs;
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
    NavigatorPlugin plugin = NavigatorPlugin.fromConfigFile(args[0]);
    NavApiCient client = plugin.getClient();

    // For the example we just take the first one without checking
    Source fs = client.getSourcesForType(SourceType.HDFS).iterator().next();

    // send tags for multiple entities to Navigator
    /*HdfsEntity dir = new HdfsEntity("/user/oozie/share/lib", EntityType
        .DIRECTORY,EnT
        fs.getIdentity());*/

    HdfsEntity dir = new HdfsEntity("/user/hdfs/hacky8", EntityType
        .DIRECTORY,
        fs.getIdentity());
    dir.setTags(Sets.newHashSet("ASJHJSA",
        "WHOHOH"));

    //dir.setIdAttrs(attrs);

    //IdAttrs attrs = new IdAttrs();
    //attrs.setFileSystemPath("/user/hdfs/hackathon");
    //dir.setIdAttrs(attrs);
    //String id = client.getEntityId(dir);

    // Write metadata
    ResultSet results = plugin.write(dir);

    if (results.hasErrors()) {
      throw new RuntimeException(results.toString());
    }
  }
}
