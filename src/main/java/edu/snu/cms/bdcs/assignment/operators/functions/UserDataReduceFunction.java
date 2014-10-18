package edu.snu.cms.bdcs.assignment.operators.functions;

import com.microsoft.reef.io.network.group.operators.Reduce;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Group all the data by the user id
 */
public class UserDataReduceFunction implements Reduce.ReduceFunction<Map<Integer, Map<Integer, Long>>> {
  private static final Logger LOG = Logger.getLogger(UserDataReduceFunction.class.getName());

  @Inject
  public UserDataReduceFunction() {
  }

  @Override
  public Map<Integer, Map<Integer, Long>> apply(Iterable<Map<Integer, Map<Integer, Long>>> elements) {
    // result : contains the data grouped by user
    Map<Integer, Map<Integer, Long>> result = new HashMap<>();
    for (Map<Integer, Map<Integer, Long>> element : elements) {
      for (int key : element.keySet()) {
        if(!result.containsKey(key)) {
          // If there is no {Item, Rate} mapping added in the result, just put the mapping for this user
          result.put(key, element.get(key));
        } else {
          // If there exists {Item, Rate} mapping, add all the mappings to the result
          Map<Integer, Long> entry = result.get(key);
          entry.putAll(element.get(key));
        }
      }
    }
//    TODO Change them into SparseVector!
//    for (int key : result.keySet()) {
//      LOG.log(Level.INFO, "User {0}th : {1}", new Object[]{key, result.get(key)});
//    }
    return result;
  }
}
