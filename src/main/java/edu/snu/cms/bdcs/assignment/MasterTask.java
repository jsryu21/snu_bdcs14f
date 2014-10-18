package edu.snu.cms.bdcs.assignment;

import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import com.microsoft.reef.task.Task;
import edu.snu.cms.bdcs.assignment.operators.AllCommunicationGroup;
import edu.snu.cms.bdcs.assignment.operators.ControlMessageBroadcaster;
import edu.snu.cms.bdcs.assignment.operators.FeatureBroadcaster;
import edu.snu.cms.bdcs.assignment.operators.InputReducer;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;

import javax.inject.Inject;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * Controller Task to control tasks for computation
 */
public class MasterTask implements Task {
  public static final String TASK_ID = "MasterTask";
  private final CommunicationGroupClient communicationGroup;
  private final Broadcast.Sender<ControlMessages> controlMessageBroadcaster;
  private final Reduce.Receiver<Map> inputReducer;
  private final Broadcast.Sender<Matrix> featureBroadcaster;


  private Matrix itemMatrix;
  private Matrix userMatrix;

  @Inject
  public MasterTask(
    final GroupCommClient groupCommClient) {
    communicationGroup = groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    this.controlMessageBroadcaster = communicationGroup.getBroadcastSender(ControlMessageBroadcaster.class);
    this.inputReducer = communicationGroup.getReduceReceiver(InputReducer.class);
    this.featureBroadcaster = communicationGroup.getBroadcastSender(FeatureBroadcaster.class);
  }

  @Override
  public byte[] call(byte[] memento) throws Exception {
    for(int iteration = 1; !converged(iteration); ++iteration) {

      controlMessageBroadcaster.send(ControlMessages.ReduceInput);
      inputReducer.reduce();



      // Compute User Matrix with Items Matrix
//      controlMessageBroadcaster.send(ControlMessages.ComputeUser);
//      featureBroadcaster.send(itemMatrix);



    }
    controlMessageBroadcaster.send(ControlMessages.Stop);

    return "Done with Master".getBytes(Charset.forName("UTF-8"));
  }

  private boolean converged(int iteration) {
    return iteration > 1000;
  }

  private void initItemMatrix(final int numFeat, final int numItems, long averageRating) {
    itemMatrix = new DenseMatrix(numFeat, numItems);
    // TODO Fill out the matrices
  }
}
