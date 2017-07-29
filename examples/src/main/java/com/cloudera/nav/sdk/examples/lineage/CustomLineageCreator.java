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

import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.writer.ResultSet;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.entities.PigOperation;
import com.cloudera.nav.sdk.model.entities.PigOperationExecution;

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
    CustomLineageCreator lineageCreator = new CustomLineageCreator(args[0]);
    lineageCreator.run();
  }

  protected final NavigatorPlugin plugin;
  private String pigOperationId;
  private String pigExecutionId;

  public CustomLineageCreator(String configFilePath) {
    this.plugin = NavigatorPlugin.fromConfigFile(configFilePath);
  }

  public void run() {
    // register all models in example
    plugin.registerModels(getClass().getPackage().getName());
    NavApiCient client = plugin.getClient();

    PigOperation pigOperation = new PigOperation(
        "44894c17c795256cc930b44702c40a0e",
        "PigLatin:id.pig");
    StetsonScript script = createStetsonScript(pigOperation);


    PigOperationExecution pigExecution = new PigOperationExecution(
        "1f802d86-fdd8-4bd9-a4ef-d82683da9fe5",
        "PigLatin:id.pig");
    StetsonExecution exec = createExecution(pigExecution);

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

  protected StetsonScript createStetsonScript(Entity pigOperaion) {
    StetsonScript script = new StetsonScript(plugin.getNamespace());
    script.setPigOperation(pigOperaion);
    script.setName("Stetson Script");
    script.setOwner("Chang");
    script.setDescription("I am a custom operation template");
    return script;
  }

  protected StetsonExecution createExecution(Entity pigExecution) {
    StetsonExecution exec = new StetsonExecution(plugin.getNamespace());
    exec.setPigExecution(pigExecution);
    exec.setName("Stetson Execution");
    exec.setDescription("I am a custom operation instance");
    exec.setLink("http://hasthelargehadroncolliderdestroyedtheworldyet.com/");
    exec.setIndex(10);
    exec.setSteward("chang");
    exec.setStarted(Instant.now());
    exec.setEnded((new Instant(Instant.now().toDate().getTime() + 10000)));
    return exec;
  }
}
