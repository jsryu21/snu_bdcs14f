/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cms.bdcs.assignment;

import com.microsoft.reef.examples.nggroup.bgd.math.DenseVector;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import com.microsoft.reef.io.network.util.Pair;
import com.microsoft.reef.task.Task;
import com.microsoft.tang.annotations.Parameter;
import edu.snu.cms.bdcs.assignment.data.RateList;
import edu.snu.cms.bdcs.assignment.operators.*;
import edu.snu.cms.bdcs.assignment.operators.functions.MaxIndexBroadcaster;

import javax.inject.Inject;
import java.util.Map;
import java.util.logging.Logger;

/**
 * A 'hello REEF' Task.
 */
public final class SlaveTask implements Task {
  private static final Logger LOG = Logger.getLogger(SlaveTask.class.getName());

  private final CommunicationGroupClient communicationGroup;
  private final Broadcast.Receiver<ControlMessages> controlMessageBroadcaster;
  private final Reduce.Sender<Pair<Integer, Pair<Integer, Integer>>> maxIndexReducer;
  private final Broadcast.Receiver<Integer> maxIndexBroadcaster;

  private final Reduce.Sender<Map<Integer, Map<Integer, Byte>>> userDataReducer;
  private final Broadcast.Receiver<Map<Integer, Map<Integer, Byte>>> userDataBroadcaster;

  private final Reduce.Sender<Map<Integer, Map<Integer, Byte>>> itemDataReducer;
  private final Broadcast.Receiver<Map<Integer, Map<Integer, Byte>>> itemDataBroadcaster;

  private final Broadcast.Receiver<Map<Integer, DenseVector>> featureMatrixBroadcaster;
  private final Reduce.Sender<Map<Integer, DenseVector>> featureMatrixReducer;

  private Map<Integer, Map<Integer, Byte>> rowRates = null, colRates = null;

  private final RateList dataSet;

  /**
   * task index to distribute the computation and data holding
   */
  private final int taskId;

  private int totalTask = 0;

  @Inject
  SlaveTask(final RateList dataSet,
            final GroupCommClient groupCommClient,
            final @Parameter(ALS.TaskIndex.class) int taskId) {
    this.dataSet = dataSet;
    this.communicationGroup = groupCommClient.getCommunicationGroup(ALSDriver.AllCommunicationGroup.class);
    this.controlMessageBroadcaster = communicationGroup.getBroadcastReceiver(ControlMessageBroadcaster.class);

    this.maxIndexReducer = communicationGroup.getReduceSender(MaxIndexReducer.class);
    this.maxIndexBroadcaster = communicationGroup.getBroadcastReceiver(MaxIndexBroadcaster.class);

    this.userDataReducer = communicationGroup.getReduceSender(UserDataReducer.class);
    this.userDataBroadcaster = communicationGroup.getBroadcastReceiver(UserDataBroadcaster.class);

    this.itemDataReducer = communicationGroup.getReduceSender(UserDataReducer.class);
    this.itemDataBroadcaster = communicationGroup.getBroadcastReceiver(UserDataBroadcaster.class);

    this.featureMatrixBroadcaster = communicationGroup.getBroadcastReceiver(FeatureMatrixBroadcaster.class);
    this.featureMatrixReducer = communicationGroup.getReduceSender(FeatureMatrixReducer.class);

    this.taskId = taskId;
    LOG.info("This is slave Task with ID"+ taskId);
  }

  @Override
  public final byte[] call(final byte[] memento) throws Exception {
    loadData();

    Map<Integer, DenseVector> itemMatrix = null;
    Map<Integer, DenseVector> userMatrix = null;

    for (boolean repeat = true; repeat; ) {

      final ControlMessages message = controlMessageBroadcaster.receive();
      switch (message) {

        case Stop:
          LOG.info("Get STOP control massage. Terminate");
          repeat = false;
          break;

        // Get maximum indices for user and item data
        case GetMaxIndex:
          final int maxUid = dataSet.getMaxUid();
          final int maxIid = dataSet.getMaxIid();
          LOG.info("Get the maximum indices : " + taskId + ", " + maxUid + " x " + maxIid);

          final Pair maxIdP = new Pair<>(taskId, new Pair<>(maxUid, maxIid));
          maxIndexReducer.send(maxIdP);
          break;

        case DistributeMaxIndex:
          totalTask = maxIndexBroadcaster.receive();
          LOG.info("Got total number of tasks : "+totalTask);
          break;

        // Collect the data ordered by UserId
        case CollectUserData:
          LOG.info("Collect the data group by UID");
          userDataReducer.send(dataSet.getUserRate());
          break;

        // Re-distribute the user data
        case DistributeUserData:
          Map userData = userDataBroadcaster.receive();
          LOG.info("Distribute the data grouped by UID. The size : "+userData.size());
          dataSet.addUserData(userData);
          break;

        // Collect the data ordered by ItemId
        case CollectItemData:
          LOG.info("Collect the data group by IID");
          itemDataReducer.send(dataSet.getItemRate());
          break;

        // Re-distribute the user data
        case DistributeItemData:
          Map itemData = itemDataBroadcaster.receive();
          LOG.info("Distribute the data grouped by IID. The size : "+itemData.size());
          dataSet.addItemData(itemData);
          break;

        case DistributeItemFeatureMatrix:
          itemMatrix = featureMatrixBroadcaster.receive();
          // TODO Now have to compute U from M
          LOG.info("Distribute the feature matrix of Item(M). The size : "+itemMatrix.size());
          break;

        case DistributeUserFeatureMatrix:
          userMatrix = featureMatrixBroadcaster.receive();
          LOG.info("Distribute the feature matrix of User(U). The size : "+userMatrix.size());
          break;

        case CollectUserFeatureMatrix:
          LOG.info("Collect the feature matrix of User(U)");
          assert(userMatrix != null);
//          featureMatrixReducer.send(userMatrix);
          featureMatrixReducer.send(itemMatrix);
//          dataSet.clearUserData();
          break;

        case CollectItemFeatureMatrix:
          LOG.info("Collect the feature matrix of Item(M)");
          assert(itemMatrix != null);
          featureMatrixReducer.send(itemMatrix);
//          dataSet.clearItemData();
          break;

        default:
          break;
      }
    }

    return null;
  }

  private void loadData() {
    rowRates = dataSet.getUserRate();
    colRates = dataSet.getItemRate();

    LOG.info("Loading data");
  }

}