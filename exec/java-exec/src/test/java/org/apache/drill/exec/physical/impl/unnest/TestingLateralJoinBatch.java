package org.apache.drill.exec.physical.impl.unnest;

import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.LateralJoinPOP;
import org.apache.drill.exec.physical.impl.join.LateralJoinBatch;
import org.apache.drill.exec.record.RecordBatch;

/**
 * Create a derived class so we can access the protected ctor
 */
public class TestingLateralJoinBatch extends LateralJoinBatch{
  protected TestingLateralJoinBatch(LateralJoinPOP popConfig, FragmentContext context, RecordBatch left,
      RecordBatch right) throws OutOfMemoryException {
    super(popConfig, context, left, right);
  }
}
