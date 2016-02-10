/*
 * Copyright (c) 2016 Cloudera, Inc.
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
package com.cloudera.nav.sdk.examples.lineage3;

import com.cloudera.nav.sdk.client.ClientConfig;
import com.cloudera.nav.sdk.client.MetadataQuery;
import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.ResultsBatch;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.net.util.Base64;

/**
 * This example shows how the lineage3 export API can be used to fetch
 * lineage for a entities retrieved as a result of issuing a query. The
 * lineage is streamed from the Navigator server and is written into a
 * lineage.json file, that is created in the current working directory.
 * The example calculates lineage downstream with a user provided length
 * and lineageOptions.
 * <p/>
 * Note that the following navigator options have to be set before this
 * example program can work.
 * <p/>
 * 1. navigator.api.lineage.export_enabled
 * Set it to true to enable the export API
 * <p/>
 * 2. navigator.api.lineage.export_batch_size
 * This sets the maximum number of entities for which the
 * lineage could be requested for. By default it is set to 100.
 */

public class LineageExport {

  private String configPath;
  private String query;
  private String direction;
  private String length;
  private String lineageOptions;
  private final static String LINEAGE_EXPORT_API = "lineage3/export";
  private final static String LINEAGE_EXPORT_API_QUERY_PARAMS = "?direction=%s&length=%s&lineageOptions=%s";
  private final static String DEFAULT_FILENAME = "lineage.json";
  private final static String IDENTITY = "identity";
  private final static int BATCH_SIZE = 100;
  private final String charset = StandardCharsets.UTF_8.name();

  /**
   * @param args 1. config file path
   *             2. query to fetch entities for which lineage
   *             would be downloaded.
   *             3. direction
   *             4. length
   *             5. lineage options
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 5);
    Preconditions.checkArgument("downstream".equals(args[2].toLowerCase())
        || "upstream".equals(args[2].toLowerCase()));
    Preconditions.checkArgument(Integer.parseInt(args[3]) > 0);
    Preconditions.checkArgument(Integer.parseInt(args[4]) >= -1);

    (new LineageExport(args[0], args[1], args[2].toUpperCase(), args[3], args[4])).run();
  }

  public LineageExport(String configPath, String query, String direction,
                       String length, String lineageOptions) {
    this.configPath = configPath;
    this.query = query;
    this.direction = direction;
    this.length = length;
    this.lineageOptions = lineageOptions;
  }

  public void run() throws IOException {
    NavApiCient client = NavigatorPlugin.fromConfigFile(configPath)
        .getClient();

    String cursorMark = null;
    List<Map<String, Object>> results = null;
    ResultsBatch<Map<String, Object>> rb;
    FileWriter writer = new FileWriter(DEFAULT_FILENAME);

    do {
      MetadataQuery mq = new MetadataQuery(query, BATCH_SIZE, cursorMark);
      rb = client.getEntityBatch(mq);

      results = rb.getResults();
      cursorMark = rb.getCursorMark();

      // extract out all the entityIds for which to download
      // lineage for
      Set<String> entityIds = Sets.newHashSet();
      for (Map<String, Object> entities : results) {
        entityIds.add((String) entities.get(IDENTITY));
      }

      if (entityIds.size() > 0) {
        fetchAndWriteLineage(entityIds, writer, client.getConfig());
      }
    } while (results.size() != 0);

    writer.flush();
    writer.close();
  }

  @SuppressWarnings("unchecked")
  private void fetchAndWriteLineage(Set<String> entityIds, FileWriter fw,
                                    ClientConfig config) throws IOException {
    HttpURLConnection connection = createHttpConnection(config, entityIds);

    try {
      InputStream stream = connection.getInputStream();

      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
      Collection<Map<String, Object>> entities = mapper.readValue(stream,
          new TypeReference<Collection<Map<String, Object>>>() {});

      for (Map<String, Object> m : entities) {
        String id = (String) m.get("id");
        Map<String, Object> children = (Map<String, Object>) m.get("children");

        if (children != null) {
          Collection<String> childrenIds;
          for (Map.Entry<String, Object> entry : children.entrySet()) {
            childrenIds = (Collection<String>) entry.getValue();

            for (String childId : childrenIds) {
              fw.write(String.format("%s, %s, %s%n", id, childId, entry.getKey()));
            }
          }
        }
      }
    } catch (IOException ioe) {
      Throwables.propagate(ioe);
    } finally {
      IOUtils.close(connection);
    }
  }

  private String constructEntityIdsAsCSV(Set<String> entityIds) throws UnsupportedEncodingException {

    if (CollectionUtils.isEmpty(entityIds)) {
      return "";
    }

    Set<String> ids = Sets.newHashSet();
    for (String id : entityIds) {
      ids.add("\"" + id + "\"");
    }

    return Joiner.on(",").join(ids);
  }

  private HttpURLConnection createHttpConnection(ClientConfig config, Set<String> entityIds)
      throws IOException {
    String navUrl = config.getNavigatorUrl();
    String serverUrl = navUrl + (navUrl.endsWith("/") ? LINEAGE_EXPORT_API : "/" + LINEAGE_EXPORT_API);
    String queryParamString = String.format(LINEAGE_EXPORT_API_QUERY_PARAMS,
        URLEncoder.encode(direction, charset), // direction of lineage
        URLEncoder.encode(length, charset), // length of lineage
        URLEncoder.encode(lineageOptions, charset)); // Apply all filters

    URL url = new URL(serverUrl + queryParamString);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    String userpass = config.getUsername() + ":" + config.getPassword();
    String basicAuth = "Basic " + new String(Base64.encodeBase64(
        userpass.getBytes()));
    conn.addRequestProperty("Authorization", basicAuth);
    conn.addRequestProperty("Content-Type", "application/json");
    conn.addRequestProperty("Accept", "application/json");
    conn.setReadTimeout(0);
    conn.setRequestMethod("POST");

    // entityIds to pass in the request body
    String postData = constructEntityIdsAsCSV(entityIds);
    postData = "[" + postData + "]";
    byte[] postDataBytes = postData.toString().getBytes("UTF-8");
    conn.addRequestProperty("Content-Length", "" + Integer.toString(postData.getBytes().length));
    conn.setDoOutput(true);
    conn.getOutputStream().write(postDataBytes);

    return conn;
  }

}
