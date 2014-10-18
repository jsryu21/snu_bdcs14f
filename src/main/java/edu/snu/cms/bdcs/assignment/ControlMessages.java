package edu.snu.cms.bdcs.assignment;

import java.io.Serializable;

/**
 * Control Message between Master/Slave tasks
 */
public enum ControlMessages implements Serializable{
  ReduceInput,
  ReceiveMaxValues,
  ReDistribute,
  ComputeItem,
  ComputeUser,
  Stop
}
