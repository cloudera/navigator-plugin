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

package com.cloudera.nav.plugin.client.examples.updatedResults;

import com.cloudera.nav.plugin.client.ClientUtils;
import com.cloudera.nav.plugin.client.NavApiCient;
import com.cloudera.nav.plugin.model.SourceType;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Nadia.Wallace on 6/22/15.
 */
public class IncrementClientExamples {

  private static final Logger LOG = LoggerFactory.getLogger(IncrementClientExamples.class);
  private IncrementalExtractionSample ies;
  private NavApiCient client;
  private String marker;

  public IncrementClientExamples(IncrementalExtractionSample ies, String marker){
    this.ies = ies;
    this.client = ies.getClient();
    this.marker = marker;
  }

  public  void getHDFSEntities(){
    Iterable<Map<String, Object>> HdfsAll =
        ies.getAllUpdated(marker, "sourceType:HDFS", "").getEntities();
    Iterable<Map<String, Object>> HdfsSingleSource =
        ies.getAllUpdated(marker, "sourceType:HDFS AND " +
            "sourceUrl:http://arne2-1.ent.cloudera.com:19888", "").getEntities();
  }

  public void getHive(){
    Iterable<Map<String, Object>> hiveDb = ies.getAllUpdated(marker,
        "sourceType:HIVE AND type:DATABASE", "").getEntities();
    Iterable<Map<String, Object>> hiveTable = ies.getAllUpdated(marker,
        "sourceType:HIVE AND type:TABLE", "").getEntities();
    Iterable<Map<String, Object>> hiveView = ies.getAllUpdated(marker,
        "sourceType:HIVE AND type:VIEW", "").getEntities();
    Iterable<Map<String, Object>> hiveColumn = ies.getAllUpdated(marker,
        "sourceType:HIVE AND type:FIELD " +
        "AND orginalName:desired_column_name", "").getEntities();
    Iterable<Map<String, Object>> hiveTableToFile = ies.getAllUpdated(marker,
        "sourceType:HIVE AND type:(DIRECTORY OR FILE)",
        "endpoint1SourceType:HIVE AND endpoint2SourceType: HIVE " +
        "AND type:PARENT_CHILD AND endpoint1Type: DIRECTORY " +
        "AND endpoint2Type: FILE").getRelations();

    //Further data processing with iterable.iterator()
  }

  public void getHiveOperations(){
    Iterable<Map<String, Object>> hiveOperationEntities = ies.getAllUpdated(marker,
        "sourceType: HIVE AND type:OPERATION_EXECUTION", "").getEntities();
    Iterable<Map<String, Object>> hiveOperationRelations = ies.getAllUpdated(marker,
        "sourceType: HIVE AND type:OPERATION_EXECUTION", "type:LOGICAL_PHYSICAL " +
        "AND endpoint1SourceType:HIVE AND endpoint1Type:OPERATION_EXECUTION").getRelations();

    //Further data processing with iterable.iterator()
  }

  public void getPigOperations(){
    throw new UnsupportedOperationException();
  }

  public void getSqoop(){
    throw new UnsupportedOperationException();
  }

  public void getMRandYarn(){
    Iterable<Map<String, Object>> yarnOperationEntities = ies.getAllUpdated(marker,
        "sourceType:(MAPREDUCE OR YARN) AND type:OPERATION_EXECUTION", "").getEntities();
    //Alternative with queryBuilder
    List<String> sourceTypes = Lists.newArrayList("MAPREDUCE", "YARN");
    List<String> types = Lists.newArrayList("OPERATION EXECUTION");
    String entityQuery = ClientUtils.queryBuilder(sourceTypes, types);
    Iterable<Map<String, Object>> yarnOperationEntities2 = ies.getAllUpdated(marker,
        entityQuery, "").getEntities();
    Iterable<Map<String, Object>> yarnOperationRelations = ies.getAllUpdated(marker,
        "sourceType: (MAPREDUCE OR YARN) AND type:OPERATION_EXECUTION", "type:DATA_FLOW " +
            "AND endpoint1SourceType:HDFS OR endpoint2SourceType:HDFS").getRelations();

    //Further data processing with iterable.iterator()
  }

  public void getOozieOperations(){ throw new UnsupportedOperationException(); }

}
