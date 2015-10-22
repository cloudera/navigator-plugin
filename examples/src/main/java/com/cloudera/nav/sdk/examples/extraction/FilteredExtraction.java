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
package com.cloudera.nav.sdk.examples.extraction;

import com.cloudera.nav.sdk.client.MetadataExtractor;
import com.cloudera.nav.sdk.client.MetadataResultSet;
import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.QueryUtils;
import com.cloudera.nav.sdk.model.Source;
import com.cloudera.nav.sdk.model.SourceType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Built on top of IncrementalExtraction, this is a sample program that runs
 * incremental extraction with filters on fields like sourceType and
 * type (entityType) to show extraction for just HDFS or Hive entities
 *
 * Program arguments:
 * 1. path to config file: see examples/src/main/resources/sample.conf
 * 2. output path: where to write the extracted marker for next run
 * 3. marker (optional): path to saved marker
 *
 * The marker is a string cursor return by the server when extraction occurs.
 * By giving a start/end marker, only metadata between the start/end markers
 * are retrieved
 */
public class FilteredExtraction {

  public static void main(String[] args) throws IOException {
    // handle arguments
    Preconditions.checkArgument(args.length >= 2);
    String configFilePath = args[0];
    String markerPath = args[1];
    String marker = args.length > 2 ?
        IncrementalExtraction.readFileArg(args[2]) : null;

    NavApiCient client = NavigatorPlugin.fromConfigFile(configFilePath)
        .getClient();

    MetadataExtractor extractor = new MetadataExtractor(client, null);

    // Run filtered examples
    getHDFSEntities(client, extractor, marker);
    getHive(extractor, marker, "city_id");
    getHiveOperations(extractor, marker);
    String nextMarker = getMRandYarn(extractor, marker);

    // Save the last marker
    try (PrintWriter writer = new PrintWriter(markerPath, "UTF-8")) {
      writer.println(nextMarker);
    }
  }

  public static void getHDFSEntities(NavApiCient client,
                                     MetadataExtractor extractor,
                                     String marker) {
    Iterable<Map<String, Object>> HdfsAll =
        extractor.extractMetadata(marker, null, "sourceType:HDFS", null)
            .getEntities();
    getFirstResult(HdfsAll);

    Source hdfsSource = client.getOnlySource(SourceType.HDFS);
    Iterable<Map<String, Object>> HdfsSingleSource =
        extractor.extractMetadata(marker, null,  "sourceType:HDFS AND " +
            "sourceId:" + hdfsSource.getIdentity(), null).getEntities();
    getFirstResult(HdfsSingleSource);
  }

  public static void getHive(MetadataExtractor extractor,
                             String marker, String colName) {
    Iterable<Map<String, Object>> hiveDb = extractor.extractMetadata(marker,
        null, "sourceType:HIVE AND type:DATABASE", null).getEntities();
    getFirstResult(hiveDb);

    Iterable<Map<String, Object>> hiveTable = extractor.extractMetadata(marker,
        null, "sourceType:HIVE AND type:TABLE", null).getEntities();
    getFirstResult(hiveTable);

    Iterable<Map<String, Object>> hiveView = extractor.extractMetadata(marker,
        null, "sourceType:HIVE AND type:VIEW", null).getEntities();
    getFirstResult(hiveView);

    Iterable<Map<String, Object>> hiveColumn = extractor.extractMetadata(marker,
        null, "sourceType:HIVE AND type:FIELD " +
        "AND originalName:" + colName, null).getEntities();
    getFirstResult(hiveColumn);

    Iterable<Map<String, Object>> hiveRelation = extractor.extractMetadata(
        marker, null,
        "sourceType:HIVE AND type:(DIRECTORY OR FILE)",
        "endpoint1SourceType:HIVE AND endpoint2SourceType: HIVE " +
        "AND type:PARENT_CHILD AND endpoint1Type: DIRECTORY " +
        "AND endpoint2Type: FILE").getRelations();
    getFirstResult(hiveRelation);
    //Further data processing with iterable.iterator()
  }

  public static void getHiveOperations(MetadataExtractor extractor,
                                       String marker) {
    Iterable<Map<String, Object>> hiveOpEntities = extractor.extractMetadata(
        marker, null, "sourceType:HIVE AND type:OPERATION_EXECUTION", null)
        .getEntities();
    getFirstResult(hiveOpEntities);

    Iterable<Map<String, Object>> hiveOpRelations = extractor.extractMetadata(
            marker, null,
            "sourceType:HIVE AND type:OPERATION_EXECUTION",
            "type:LOGICAL_PHYSICAL AND endpoint1SourceType:HIVE " +
            "AND endpoint1Type:OPERATION_EXECUTION").getRelations();
    getFirstResult(hiveOpRelations);
    //Further data processing with iterable.iterator()
  }

  public static String getMRandYarn(MetadataExtractor extractor,
                                    String marker) {
    Iterable<Map<String, Object>> yarnOpEntities = extractor.extractMetadata(
        marker, null, "sourceType:(MAPREDUCE OR YARN) AND type:OPERATION_EXECUTION",
        null).getEntities();
    getFirstResult(yarnOpEntities);

    //Alternative with buildQuery
    List<String> sourceTypes = Lists.newArrayList("MAPREDUCE", "YARN");
    List<String> types = Lists.newArrayList("OPERATION EXECUTION");
    String entityQuery = QueryUtils.buildQuery(sourceTypes, types);
    Iterable<Map<String, Object>> yarnOpEntities2 = extractor.extractMetadata(
        marker, null, entityQuery, "").getEntities();
    getFirstResult(yarnOpEntities2);

    MetadataResultSet yarnOp = extractor.extractMetadata(
        marker, null,
        "sourceType:(MAPREDUCE OR YARN) AND type:OPERATION_EXECUTION",
        "type:DATA_FLOW AND endpoint1SourceType:HDFS OR endpoint2SourceType:HDFS");
    Iterable<Map<String, Object>> yarnOpRelations = yarnOp.getRelations();
    getFirstResult(yarnOpRelations);
    //Further data processing with iterable.iterator()
    return yarnOp.getMarker();
  }

  private static void getFirstResult(Iterable<Map<String, Object>> iterable) {
    // In real usage, iterate through results and process each metadata object
    Iterator<Map<String, Object>> iterator = iterable.iterator();
    if(iterator.hasNext()) {
      Map<String, Object> result = iterator.next();
      System.out.println("source: " + result.get("sourceType") +
          "  type: " + result.get("type"));
    } else {
      System.out.println("no elements found");
    }
  }
}
