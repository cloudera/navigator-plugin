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
import com.cloudera.nav.plugin.client.PluginConfigurationFactory;
import com.cloudera.nav.plugin.client.PluginConfigurations;
import com.cloudera.nav.plugin.model.SourceType;
import com.google.common.collect.Lists;
import com.google.common.base.Throwables;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**Example calls using the IncrementalExtractionSample class to perform
 * incremental extraction. Examples shown utilise the getAllUpdated() signature
 * with a marker and with query strings for specifying Entities and Relations
 * to be retrieved.
 *
 */
public class IncrementClientExamples {

  private static String marker = "{\"4fb48492c18e21ae5be9c2f7faeffe62\":600," +
      "\"4fbdadc6899638782fc8cb626176dc7b\":600," +
      "\"efd3b52ca1bebb19990a0a92c7ff6b89\":600," +
      "\"a063e69e6c0660353dc378c836837935\":600," +
      "\"a09b0233cc58ff7d601eaa68673a20c6\":600}";

  public static void getHDFSEntities(IncrementalExtractionSample ies, String marker){
    Iterable<Map<String, Object>> HdfsAll =
        ies.getAllUpdated(marker, "sourceType:HDFS", "").getEntities();
    getFirstResult(HdfsAll);

    Iterable<Map<String, Object>> HdfsSingleSource =
        ies.getAllUpdated(marker, "sourceType:HDFS AND " +
            "sourceUrl:http://arne2-1.ent.cloudera.com:19888", "").getEntities();
    getFirstResult(HdfsSingleSource);
  }

  public static void getHive(IncrementalExtractionSample ies,
                             String marker,
                             String colName){
    Iterable<Map<String, Object>> hiveDb = ies.getAllUpdated(marker,
        "sourceType:HIVE AND type:DATABASE", "").getEntities();
    getFirstResult(hiveDb);

    Iterable<Map<String, Object>> hiveTable = ies.getAllUpdated(marker,
        "sourceType:HIVE AND type:TABLE", "").getEntities();
    getFirstResult(hiveTable);

    Iterable<Map<String, Object>> hiveView = ies.getAllUpdated(marker,
        "sourceType:HIVE AND type:VIEW", "").getEntities();
    getFirstResult(hiveView);

    Iterable<Map<String, Object>> hiveColumn = ies.getAllUpdated(marker,
        "sourceType:HIVE AND type:FIELD " +
        "AND originalName:"+colName, "").getEntities();
    getFirstResult(hiveColumn);

    Iterable<Map<String, Object>> hiveTableToFile = ies.getAllUpdated(marker,
        "sourceType:HIVE AND type:(DIRECTORY OR FILE)",
        "endpoint1SourceType:HIVE AND endpoint2SourceType: HIVE " +
        "AND type:PARENT_CHILD AND endpoint1Type: DIRECTORY " +
        "AND endpoint2Type: FILE").getRelations();
    getFirstResult(hiveTableToFile);

    //Further data processing with iterable.iterator()
  }

  public static void getHiveOperations(IncrementalExtractionSample ies, String marker){
    Iterable<Map<String, Object>> hiveOperationEntities = ies.getAllUpdated(marker,
        "sourceType: HIVE AND type:OPERATION_EXECUTION", "").getEntities();
    getFirstResult(hiveOperationEntities);

    Iterable<Map<String, Object>> hiveOperationRelations = ies.getAllUpdated(marker,
        "sourceType: HIVE AND type:OPERATION_EXECUTION", "type:LOGICAL_PHYSICAL " +
        "AND endpoint1SourceType:HIVE AND endpoint1Type:OPERATION_EXECUTION").getRelations();
    getFirstResult(hiveOperationRelations);
    //Further data processing with iterable.iterator()
  }

  public static void getPigOperations(){ throw new UnsupportedOperationException(); }

  public static void getSqoop(){
    throw new UnsupportedOperationException();
  }

  public static void getMRandYarn(IncrementalExtractionSample ies, String marker){
    Iterable<Map<String, Object>> yarnOperationEntities = ies.getAllUpdated(marker,
        "sourceType:(MAPREDUCE OR YARN) AND type:OPERATION_EXECUTION", "").getEntities();
    getFirstResult(yarnOperationEntities);

    //Alternative with queryBuilder
    List<String> sourceTypes = Lists.newArrayList("MAPREDUCE", "YARN");
    List<String> types = Lists.newArrayList("OPERATION EXECUTION");
    String entityQuery = ClientUtils.queryBuilder(sourceTypes, types);
    Iterable<Map<String, Object>> yarnOperationEntities2 = ies.getAllUpdated(marker,
        entityQuery, "").getEntities();
    getFirstResult(yarnOperationEntities2);

    Iterable<Map<String, Object>> yarnOperationRelations = ies.getAllUpdated(marker,
        "sourceType:(MAPREDUCE OR YARN) AND type:OPERATION_EXECUTION", "type:DATA_FLOW " +
            "AND endpoint1SourceType:HDFS OR endpoint2SourceType:HDFS").getRelations();
    getFirstResult(yarnOperationRelations);
    //Further data processing with iterable.iterator()
  }

  public static void getOozieOperations(){ throw new UnsupportedOperationException(); }

  private static void getFirstResult(Iterable<Map<String, Object>> iterable){
    Iterator<Map<String, Object>> iterator = iterable.iterator();
    if(iterator.hasNext()) {
      Map<String, Object> result = iterator.next();
      System.out.println("source: " + result.get("sourceType") + "\n type: " + result.get("type"));
    } else {
      System.out.println("no elements found");
    }
  }

  public static  void main(String[] args){
    String configFilePath = args[0];
    PluginConfigurations config = (new PluginConfigurationFactory())
        .readConfigurations(configFilePath);
    NavApiCient client = new NavApiCient(config);
    IncrementalExtractionSample ies = new IncrementalExtractionSample(client);
    String firstMarker = ies.getAllUpdated().getMarker();
    //marker = firstMarker;
//    try {
//      Thread.sleep(60000);
//    } catch(InterruptedException e) {
//      throw Throwables.propagate(e);
//    }
    getHDFSEntities(ies, "");
    getHive(ies, "", "city_id");
    getHiveOperations(ies, "");
    getMRandYarn(ies, "");
  }

}
