package edu.snu.cms.bdcs.assignment.operators.functions;

import com.microsoft.reef.examples.nggroup.bgd.math.DenseVector;
import com.microsoft.reef.io.network.group.operators.Reduce;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Group all the data by the user id
 */
public class FeatureMatrixReduceFunction implements Reduce.ReduceFunction<Map<Integer, DenseVector>> {
  private static final Logger LOG = Logger.getLogger(FeatureMatrixReduceFunction.class.getName());

  @Inject
  public FeatureMatrixReduceFunction() {
  }


//    TODO Change them into SparseVector!
//    for (int key : result.keySet()) {
//      LOG.log(Level.INFO, "User {0}th : {1}", new Object[]{key, result.get(key)});
//    }

  @Override
  public Map<Integer, DenseVector> apply(Iterable<Map<Integer, DenseVector>> elements) {
    final Map result = new HashMap<Integer, DenseVector>();
    for(Map<Integer, DenseVector> element: elements) {
      result.putAll(element);
    }
    return result;
  }
}
