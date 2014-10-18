package edu.snu.cms.bdcs.assignment.operators.functions;

import com.microsoft.reef.io.network.group.operators.Reduce;

import java.util.Map;

/**
 * Created by yunseong on 10/18/14.
 */
public class InputReduceFunction implements Reduce.ReduceFunction<Map<Long, Map<Long, Long>>> {
  @Override
  public Map<Long, Map<Long, Long>> apply(Iterable<Map<Long, Map<Long, Long>>> elements) {
    return null;
  }
}
