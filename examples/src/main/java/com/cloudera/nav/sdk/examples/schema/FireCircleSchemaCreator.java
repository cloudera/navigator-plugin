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

    // initialize the plugin
    NavigatorPlugin plugin = NavigatorPlugin.fromConfigFile(args[0]);

    // register models in package
    plugin.registerModels("com.cloudera.nav.sdk.examples.schema");

    // get the HDFS source
    Source fs = plugin.getClient().getSourcesForType(SourceType.HDFS)
        .iterator().next();

    // specify the HDFS directory that contains the data
    HdfsEntity container = new HdfsEntity(args[1], EntityType.DIRECTORY,
        fs.getIdentity());

    FireCircleDataset dataset = new FireCircleDataset();
    dataset.setName("My Dataset");
    dataset.setDataContainer(container);
    dataset.setFileFormat(FileFormat.CSV);
    // "4","4","2","","2008-07-31 00:00:00",""
    dataset.setFields(ImmutableList.of(
        new FireCircleField("col1", "integer"),
        new FireCircleField("col2", "integer"),
        new FireCircleField("col3", "integer"),
        new FireCircleField("col4", "string"),
        new FireCircleField("col5", "date"),
        new FireCircleField("col6", "string")
    ));
    // Write metadata
    ResultSet results = plugin.write(dataset);

    if (results.hasErrors()) {
      throw new RuntimeException(results.toString());
    }
  }
}
