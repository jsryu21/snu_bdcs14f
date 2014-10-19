package edu.snu.cms.bdcs.assignment.operators.functions;

import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.util.Pair;

import javax.inject.Inject;

/**
 * Resolve the number of task id and user id and item id
 */
public class MaxIndexReduceFunction implements Reduce.ReduceFunction<Pair<Integer, Pair<Integer, Integer>>> {
  @Inject
  public MaxIndexReduceFunction() {

  }

  // TODO : maybe we need to change to collect unique keys they have
  @Override
  public Pair<Integer, Pair<Integer, Integer>> apply(Iterable<Pair<Integer, Pair<Integer, Integer>>> elements) {
    int maxTaskId = 0, maxUid = 0, maxIid = 0;

    for (Pair<Integer, Pair<Integer, Integer>> element : elements) {
      if(maxTaskId < element.first)
        maxTaskId = element.first;

      Pair<Integer, Integer> UidIidP = element.second;
      if (maxUid < UidIidP.first)
        maxUid = UidIidP.first;
      if (maxIid < UidIidP.second)
        maxIid = UidIidP.second;
    }
    return new Pair<>(maxTaskId, new Pair<>(maxUid, maxIid));
  }
}