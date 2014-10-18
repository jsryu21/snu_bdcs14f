package edu.snu.cms.bdcs.assignment;

import java.io.Serializable;

/**
 * Control Message between Master/Slave tasks
 */
public enum ControlMessages implements Serializable{
  GetMaxIndex,
  CollectUserData,
  DistributeUserData,
  CollectItemData,
  DistributeItemData,
  DistributeItemFeatureMatrix,
  DistributeUserFeatureMatrix,
  CollectItemFeatureMatrix,
  CollectUserFeatureMatrix,
  Stop
}
