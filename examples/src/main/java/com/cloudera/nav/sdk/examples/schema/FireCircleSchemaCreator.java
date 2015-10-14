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

package com.cloudera.nav.sdk.examples.schema;

import java.util.HashMap;
import java.util.Map;

import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.writer.ResultSet;
import com.cloudera.nav.sdk.model.Source;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.cloudera.nav.sdk.model.entities.FileFormat;
import com.cloudera.nav.sdk.model.entities.HdfsEntity;
import com.google.common.collect.ImmutableList;

/**
 * In this example, a hypothetical application called FireCircle infers
 * schema for HDFS directories and would like to add that schema information
 * to Navigator for joint users to view. We create the schema as a
 * {@link FireCircleDataset} that can
 * contain 0 or more fields.
 */
public class FireCircleSchemaCreator {

  /**
   * We assume the directory name is the dataset container
   * @param args
   */
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

    // get the HDFS source
    Source fs = plugin.getClient().getOnlySource(SourceType.HDFS);

    // specify the HDFS directory that contains the data
    String path = "/user/talend/dataoutput";
    HdfsEntity container = new HdfsEntity(path, EntityType.DIRECTORY,
        fs.getIdentity());

    FireCircleDataset dataset = new FireCircleDataset();
    dataset.setName("Talend Studio Output Dataset");
    dataset.setDataContainer(container);
    dataset.setFileFormat(FileFormat.CSV);
    // "4","4","2","","2008-07-31 00:00:00",""
    dataset.setFields(ImmutableList.of(
        new FireCircleField("b", "String"),
        new FireCircleField("c", "integer")

    ));
    // Write metadata
    ResultSet results = plugin.write(dataset);

    if (results.hasErrors()) {
      throw new RuntimeException(results.toString());
    }
  }
}
