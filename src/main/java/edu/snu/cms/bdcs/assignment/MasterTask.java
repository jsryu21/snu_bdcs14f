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
import edu.snu.cms.bdcs.assignment.operators.functions.MaxIndexBroadcaster;

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
  private final Reduce.Receiver<Pair<Integer, Pair<Integer, Integer>>> maxIndexReducer;
  private final Broadcast.Sender<Pair<Integer, Pair<Integer, Integer>>> maxIndexBroadcaster;

  private final Reduce.Receiver<Map<Integer, Map<Integer, Byte>>> userDataReducer;
  private final Broadcast.Sender<Map<Integer, Map<Integer, Byte>>> userDataBroadcaster;

  private final Reduce.Receiver<Map<Integer, Map<Integer, Byte>>> itemDataReducer;
  private final Broadcast.Sender<Map<Integer, Map<Integer, Byte>>> itemDataBroadcaster;

  private final Broadcast.Sender<Map<Integer, DenseVector>> featureMatrixBroadcaster;
  private final Reduce.Receiver<Map<Integer, DenseVector>> featureMatrixReducer;

  private Pair<Integer, Pair<Integer, Integer>> maxIndexP;
  private double errorRate = Double.MAX_VALUE;

  private Map<Integer, DenseVector> itemMatrix = null;
  private Map<Integer, DenseVector> userMatrix = null;

  private final int numFeat;
  private final int maxIter = 2;

  private final double eta = 0.0001;

  @Inject
  public MasterTask(
    final GroupCommClient groupCommClient,
    final @Parameter(ALS.NumFeature.class) int numFeat) {
    this.numFeat = numFeat;
    communicationGroup = groupCommClient.getCommunicationGroup(ALSDriver.AllCommunicationGroup.class);

    this.controlMessageBroadcaster = communicationGroup.getBroadcastSender(ControlMessageBroadcaster.class);
    this.maxIndexReducer = communicationGroup.getReduceReceiver(MaxIndexReducer.class);
    this.maxIndexBroadcaster = communicationGroup.getBroadcastSender(MaxIndexBroadcaster.class);

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
    final int numTask = maxIndexP.first;
    final int numUser = maxIndexP.second.first;
    final int numItem = maxIndexP.second.second;
    LOG.info("Total "+numTask+" tasks. Data size :"+ numUser +" x "+ numItem);

    controlMessageBroadcaster.send(ControlMessages.DistributeMaxIndex);
    maxIndexBroadcaster.send(new Pair(numTask, new Pair(numUser, numItem)));
    LOG.info("Send the number of tasks "+numTask);

    // 2. Collect R and group by user.
    controlMessageBroadcaster.send(ControlMessages.CollectUserData);
    final Map userData = userDataReducer.reduce(); // R grouped by U
    LOG.info("R grouped by user. The size : "+userData.keySet().size());

    // TODO Use scatter to reduce the redundancy.
    // 3. Redistribute R
    controlMessageBroadcaster.send(ControlMessages.DistributeUserData);
    userDataBroadcaster.send(userData);
    LOG.info("Distribute R grouped by user");
    userData.clear();

    // TODO The reason I split into two phase is to reduce overhead to keep replicate in one time
    // 4. Collect R and group by item.
    controlMessageBroadcaster.send(ControlMessages.CollectItemData);
    final Map itemData = itemDataReducer.reduce(); // R grouped by M
    LOG.info("R grouped by item. The size : "+itemData.keySet().size());

    // 5. Redistribute R
    controlMessageBroadcaster.send(ControlMessages.DistributeItemData);
    itemDataBroadcaster.send(itemData);
    LOG.info("Distribute R grouped by item");
    itemData.clear();

    // 6. Init ItemMatrix
    double averageRate = 2.5; // TODO Get average to initiate M

    itemMatrix = initItemMatrix(numFeat, numItem, averageRate);

    /*
     * Iteration Step
     */
    int iteration = 0;
    do {
      // Update User matrix using Item Matrix
      // Send Message : "Compute Item!"
      // Broadcast ItemFeature
      // Send Message : "Gather Ui"
      // Gather the Vectors
      // => Update User Matrix
      controlMessageBroadcaster.send(ControlMessages.DistributeItemFeatureMatrix);
      featureMatrixBroadcaster.send(itemMatrix);
      LOG.info("Broadcast M to update U. Iteration : "+iteration);

      controlMessageBroadcaster.send(ControlMessages.CollectUserFeatureMatrix);
      userMatrix = featureMatrixReducer.reduce();
      LOG.info("Collect U. Iteration : "+iteration);

      // Update Item matrix using User Matrix
      // Send Message : "Compute User!"
      // Broadcast UserFeature
      // Send Message : "Gather Ij"
      // Gather the Vectors
      // => Update Item Matrix
      controlMessageBroadcaster.send(ControlMessages.DistributeUserFeatureMatrix);
      featureMatrixBroadcaster.send(itemMatrix);

      controlMessageBroadcaster.send(ControlMessages.CollectItemFeatureMatrix);
      itemMatrix = featureMatrixReducer.reduce();

    } while(!converged(++iteration));
    /*
     * Send STOP messages to all the slave tasks.
     */
    controlMessageBroadcaster.send(ControlMessages.Stop);
    LOG.info("Converged. Let's stop the job");

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
    return iteration >= maxIter || getLossFunction() < eta;
  }

  private double getLossFunction() {
    // TODO how to hold test data
    return 0;
  }
}
