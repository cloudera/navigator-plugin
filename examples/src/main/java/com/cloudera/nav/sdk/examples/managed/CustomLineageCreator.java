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

package com.cloudera.nav.sdk.examples.managed;

import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.writer.ResultSet;
import com.cloudera.nav.sdk.model.IdAttrs;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.entities.PigOperation;
import com.cloudera.nav.sdk.model.entities.PigOperationExecution;

import org.joda.time.Instant;

/**
 * In this example we extend the lineage example to show how to create managed
 * metadata properties. For more details on the base example, please see
 * {@link com.cloudera.nav.sdk.examples.lineage.CustomLineageCreator}.
 *
 * In this example, we've created several managed properties in the
 * `StetsonExecution` classes using the `register` attribute in `@MProperty`
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
    // Create the template

    PigOperation pigOperation = new PigOperation(
        "24477434f5464a93c84c7bdc2115981f",
        "PigLatin:id.pig");

    StetsonScript script = createStetsonScript(pigOperation);

    PigOperationExecution pigOperationExecution = new PigOperationExecution(
        "345ca493-ba49-4c58-9780-fd60d8c037e0",
        "PigLatin:id.pig");

    // Create the instance
    StetsonExecution exec = createExecution(pigOperationExecution);
    // Connect the template and instance
    script.setIdentity(script.generateId());
    exec.setTemplate(script);

    // Bad steward field, does not match regex
    exec.setSteward("foo");
    assert plugin.write(exec).hasErrors();

    // correct steward
    exec.setSteward("chang@company.com");

    // Bad group
    exec.setGroup("Random Group");
    assert plugin.write(exec).hasErrors();

    // correct group
    exec.setGroup(StetsonExecution.INFRA);

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

  protected StetsonScript createStetsonScript(Entity pigOperation) {
    StetsonScript script = new StetsonScript(plugin.getNamespace());
    script.setPigOperation(pigOperation);
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
    exec.setStarted(Instant.now());
    exec.setEnded((new Instant(Instant.now().toDate().getTime() + 10000)));
    return exec;
  }
}
