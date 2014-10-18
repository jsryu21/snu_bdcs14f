package edu.snu.cms.bdcs.assignment;

import com.microsoft.reef.examples.nggroup.bgd.math.DenseVector;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import com.microsoft.reef.io.network.util.Pair;
import com.microsoft.reef.task.Task;
import com.microsoft.tang.annotations.Parameter;
import edu.snu.cms.bdcs.assignment.operators.*;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;

import javax.inject.Inject;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Controller Task to control tasks for computation
 */
public class MasterTask implements Task {
  public static final String TASK_ID = "MasterTask";
  private static final Logger LOG =Logger.getLogger(TASK_ID);

  private final CommunicationGroupClient communicationGroup;
  private final Broadcast.Sender<ControlMessages> controlMessageBroadcaster;
  private final Reduce.Receiver<Pair<Integer, Integer>> maxIndexReducer;

  private final Reduce.Receiver<Map<Integer, Map<Integer, Byte>>> userDataReducer;
  private final Broadcast.Sender<Map<Integer, Map<Integer, Byte>>> userDataBroadcaster;

  private final Reduce.Receiver<Map<Integer, Map<Integer, Byte>>> itemDataReducer;
  private final Broadcast.Sender<Map<Integer, Map<Integer, Byte>>> itemDataBroadcaster;

  private final Broadcast.Sender<Map<Integer, DenseVector>> featureMatrixBroadcaster;
  private final Reduce.Receiver<Map<Integer, DenseVector>> featureMatrixReducer;

  private Pair<Integer, Integer> maxIndexP;
  private double errorRate = Double.MAX_VALUE;

  private Map<Integer, DenseVector> itemMatrix = null;
  private Map<Integer, DenseVector> userMatrix = null;

  private final int numFeat;

  @Inject
  public MasterTask(
    final GroupCommClient groupCommClient,
    final @Parameter(ALS.NumFeature.class) int numFeat) {
    this.numFeat = numFeat;
    communicationGroup = groupCommClient.getCommunicationGroup(ALSDriver.AllCommunicationGroup.class);

    this.controlMessageBroadcaster = communicationGroup.getBroadcastSender(ControlMessageBroadcaster.class);
    this.maxIndexReducer = communicationGroup.getReduceReceiver(MaxIndexReducer.class);

    this.userDataReducer = communicationGroup.getReduceReceiver(UserDataReducer.class);
    this.userDataBroadcaster = communicationGroup.getBroadcastSender(UserDataBroadcaster.class);

    this.itemDataReducer = communicationGroup.getReduceReceiver(UserDataReducer.class);
    this.itemDataBroadcaster = communicationGroup.getBroadcastSender(UserDataBroadcaster.class);

    this.featureMatrixBroadcaster = communicationGroup.getBroadcastSender(FeatureMatrixBroadcaster.class);
    this.featureMatrixReducer = communicationGroup.getReduceReceiver(FeatureMatrixReducer.class);
  }

  @Override
  public byte[] call(byte[] memento) throws Exception {
    /*
     * Initial phase before iteration
     */

    // 1. Get size of the input
    controlMessageBroadcaster.send(ControlMessages.GetMaxIndex);
    maxIndexP = maxIndexReducer.reduce();
    LOG.info("Index :"+ maxIndexP.first+","+ maxIndexP.second);

    // 2. Collect R and group by user.
    controlMessageBroadcaster.send(ControlMessages.CollectUserData);
    final Map userData = userDataReducer.reduce(); // R grouped by U

    // TODO Use scatter to reduce the redundancy.
    // 3. Redistribute R
    controlMessageBroadcaster.send(ControlMessages.DistributeUserData);
    userDataBroadcaster.send(userData);
    userData.clear();

    // TODO The reason I split into two phase is to reduce overhead to keep replicate in one time
    // 4. Collect R and group by item.
    controlMessageBroadcaster.send(ControlMessages.CollectItemData);
    final Map itemData = itemDataReducer.reduce(); // R grouped by U

    // 5. Redistribute R
    controlMessageBroadcaster.send(ControlMessages.DistributeItemData);
    itemDataBroadcaster.send(itemData);
    itemData.clear();

    // 6. Init ItemMatrix
    double averageRate = 2.5; // TODO Get average to initiate M
    itemMatrix = initItemMatrix(numFeat, maxIndexP.second, averageRate);

    /*
     * Iteration Step
     */
    final int iteration = -1;
    do {
      // Update User matrix using Item Matrix
      // Send Message : "Compute Item!"
      // Broadcast ItemFeature
      // Send Message : "Gather Ui"
      // Gather the Vectors
      // => Update User Matrix
      controlMessageBroadcaster.send(ControlMessages.DistributeItemFeatureMatrix);
      featureMatrixBroadcaster.send(itemMatrix);

      controlMessageBroadcaster.send(ControlMessages.CollectUserFeatureMatrix);
      userMatrix = featureMatrixReducer.reduce();

      // Update Item matrix using User Matrix
      // Send Message : "Compute User!"
      // Broadcast UserFeature
      // Send Message : "Gather Ij"
      // Gather the Vectors
      // => Update Item Matrix
//      controlMessageBroadcaster.send(ControlMessages.DistributeUserFeatureMatrix);
//      featureMatrixBroadcaster.send(itemMatrix);

//      controlMessageBroadcaster.send(ControlMessages.CollectItemFeatureMatrix);
//      itemMatrix = featureMatrixReducer.reduce();

    } while(!converged(iteration));
    /*
     * Send STOP messages to all the slave tasks.
     */
    controlMessageBroadcaster.send(ControlMessages.Stop);

    final String message = "Done. Error rate is : " + errorRate;
    return message.getBytes(Charset.forName("UTF-8"));
  }

  private Map<Integer, DenseVector> initItemMatrix(final int numFeat, final int numItems, double averageRating) {
    // TODO Fill out the matrices
    Map<Integer, DenseVector> result = new HashMap<Integer, DenseVector>();
    for (int i = 0; i < numFeat; i++) {
      result.put(i, new DenseVector(numItems));
    }
    return result;
  }

  private boolean converged(int iteration) {
    // TODO Validation Step
    // TODO Update the error rate
//    return iteration > 1000;
    return true;
  }
}
