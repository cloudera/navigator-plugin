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

package com.cloudera.nav.sdk.examples.lineage;

import java.util.HashMap;
import java.util.Map;

import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.writer.ResultSet;

import org.joda.time.Instant;

/**
 * In this example we show how to create custom entity types and
 * how to link them to hadoop entities. We define a custom operation entity
 * from a hypothetical application called Stetson. The Stetson application
 * defines a custom operations called StetsonScript and a custom operation
 * execution entity called StetsonExecution. A StetsonScript is the template
 * for a StetsonExecution and so we use the @MRelation annotation to specify an
 * InstanceOf relationship between the StetsonExecution and StetsonScript.
 *
 * In Hadoop, Stetson operations are carried out using Pig. In order to
 * establish the relationship between the Stetson custom entities and the
 * hadoop entities, we again use the @MRelation annotation to form
 * LogicalPhysical relationships.
 *
 * Relations created:
 *
 * StetsonScript ---(LogicalPhysical)---> Pig Operation
 * StetsonExecution ---(LogicalPhysical)---> Pig Execution
 *
 * The relationships between Pig operation and execution are created
 * automatically by Navigator
 */
public class CustomLineageCreator {

  /**
   * @param args 1. config file path
   *             2. Pig operation id
   *             3. Pig execution id
   */
  public static void main(String[] args) {
	  
	// setup the plugin and api client
	  Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("navigator_url", "http://ip-192-168-100-9.ec2.internal:7187/api/v7/");
		configMap.put("username", "talend");
		configMap.put("password", "Cloudera!!");
		configMap.put("metadata_parent_uri", "http://ip-192-168-100-9.ec2.internal:7187/api/v7/metadata/plugin");
		configMap.put("autocommit", "true");
		configMap.put("application_url", "http://ip-192-168-100-9.ec2.internal:7187/");
		configMap.put("namespace", "talend");
		
    CustomLineageCreator lineageCreator = new CustomLineageCreator(configMap);
    
    lineageCreator.setPigOperationId("2ee7c631c5369145b91733c5c3ec7e92");
    
    lineageCreator.setPigExecutionId("2ee7c631c5369145b91733c5c3ec7e92");
    
    lineageCreator.run();
  }

  protected final NavigatorPlugin plugin;
  private String pigOperationId;
  private String pigExecutionId;

  public CustomLineageCreator(String configFilePath) {
    this.plugin = NavigatorPlugin.fromConfigFile(configFilePath);
  }
  
  public CustomLineageCreator(Map<String, Object> configMap) {
	    this.plugin = NavigatorPlugin.fromConfigMap(configMap);
	  }

  public void run() {
    // Create the template
    StetsonScript script = createStetsonScript();
    // Create the instance
    StetsonExecution exec = createExecution();
    // Connect the template and instance
    script.setIdentity(script.generateId());
    exec.setTemplate(script);
    // Write metadata
    ResultSet results = plugin.write(exec);

    if (results.hasErrors()) {
      throw new RuntimeException(results.toString());
    }
  }

  public String getPigOperationId() {
    return pigOperationId;
  }

  public String getPigExecutionId() {
    return pigExecutionId;
  }

  public void setPigOperationId(String pigOperationId) {
    this.pigOperationId = pigOperationId;
  }

  public void setPigExecutionId(String pigExecutionId) {
    this.pigExecutionId = pigExecutionId;
  }

  protected StetsonScript createStetsonScript() {
    StetsonScript script = new StetsonScript(plugin.getNamespace());
    script.setPigOperation(getPigOperationId());
    script.setName("Stetson Script");
    script.setOwner("Chang");
    script.setDescription("TALEND CUSTOM SCRIPT");
    return script;
  }

  protected StetsonExecution createExecution() {
    StetsonExecution exec = new StetsonExecution(plugin.getNamespace());
    exec.setPigExecution(getPigExecutionId());
    exec.setName("Stetson Execution");
    exec.setDescription("TALEND CUSTOM EXECUTION");
    exec.setLink("http://hasthelargehadroncolliderdestroyedtheworldyet.com/");
    exec.setStarted(Instant.now());
    exec.setEnded(Instant.now());
    return exec;
  }
}
