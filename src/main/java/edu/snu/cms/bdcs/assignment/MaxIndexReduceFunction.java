package edu.snu.cms.bdcs.assignment;

import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.util.Pair;

import javax.inject.Inject;

/**
 *
 */
public class MaxIndexReduceFunction implements Reduce.ReduceFunction<Pair<Integer, Integer>> {
  @Inject
  public MaxIndexReduceFunction() {

  }

  // TODO : maybe we need to change to collect unique keys they have
  @Override
  public Pair<Integer, Integer> apply(Iterable<Pair<Integer, Integer>> elements) {
    int maxUid = 0, maxIid = 0;

    for (Pair<Integer, Integer> element : elements) {
      if (maxUid < element.first)
        maxUid = element.first;
      if (maxIid < element.second)
        maxIid = element.second;
    }
    return new Pair<>(maxUid, maxIid);
  }
}