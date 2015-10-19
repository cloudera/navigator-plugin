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

package com.cloudera.nav.sdk.client.writer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;

import org.apache.commons.httpclient.HttpStatus;

import com.cloudera.nav.sdk.client.PluginConfigurations;
import com.cloudera.nav.sdk.client.writer.serde.EntitySerializer;
import com.cloudera.nav.sdk.client.writer.serde.RelationSerializer;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.base.Throwables;

/**
 * Write metadata in JSON format
 */
public class JsonMetadataWriter extends MetadataWriter {

  private final HttpURLConnection conn;
  private final ObjectMapper mapper;
  private ResultSet lastResult;

  public JsonMetadataWriter(PluginConfigurations config,
                            OutputStream stream,
                            HttpURLConnection conn) {
    super(config, stream);
    this.conn = conn;
    this.mapper = newMapper();
  }

  @Override
  protected void persistMetadataValues(MClassWrapper mclassWrapper) {
    try {
      mapper.writeValue(stream, mclassWrapper);
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  private ObjectMapper newMapper() {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule("MetadataSerializer");
    module.addSerializer(new EntitySerializer(registry));
    module.addSerializer(new RelationSerializer(registry));
    mapper.registerModule(module);
    mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    mapper.configure(DeserializationFeature.WRAP_EXCEPTIONS, false);
    mapper.registerModule(new JodaModule());
    return mapper;
  }

  @Override
  public void flush() {
    super.flush();
    try {
      // request is not sent until response code is requested
      if (conn.getResponseCode() >= HttpStatus.SC_BAD_REQUEST) {
    	  
    	  // display error message
    	  BufferedReader br = new BufferedReader(new InputStreamReader((conn.getErrorStream())));
    	  StringBuilder sb = new StringBuilder();
    	  String responseBody;
    	  while ((responseBody = br.readLine()) != null) {
    		  sb.append(responseBody);
    	  }
    	  responseBody = sb.toString();
    	  
        throw new RuntimeException(String.format(
            "Error writing metadata (code %s): %s %s", conn.getResponseCode(),
            conn.getResponseMessage(), responseBody));
      }
      lastResult = mapper.readValue(conn.getInputStream(), ResultSet.class);
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  @Override
  public ResultSet getLastResultSet() {
    return lastResult;
  }
}
