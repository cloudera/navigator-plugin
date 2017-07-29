package com.cloudera.nav.sdk.model.entities;

import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MEndPoint;


@MEndPoint
@MClass(model="pig_op_exec", validTypes = {EntityType.OPERATION_EXECUTION})
public class PigOperationExecution extends Entity {

  public PigOperationExecution() {
    setSourceType(SourceType.PIG);
    setEntityType(EntityType.OPERATION_EXECUTION);
  }

  public PigOperationExecution(String id) {
    this();
    setIdentity(id);
  }

  public PigOperationExecution(String scriptId, String jobName) {
    this();
    setScriptId(scriptId);
    setJobName(jobName);
    setIsIdGenerated(false);
  }
}
