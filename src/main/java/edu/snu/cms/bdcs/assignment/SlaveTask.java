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

import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import com.microsoft.reef.io.network.util.Pair;
import com.microsoft.reef.task.Task;
import edu.snu.cms.bdcs.assignment.data.RateList;
import edu.snu.cms.bdcs.assignment.operators.*;
import org.apache.mahout.math.Matrix;

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
  private final Broadcast.Receiver<Matrix> featureMatrixBroadcaster;
  private final Reduce.Sender<Pair<Integer, Integer>> maxIndexReducer;

  private final Reduce.Sender<Map<Integer, Map<Integer, Byte>>> userDataReducer;
  private final Broadcast.Receiver<Map<Integer, Map<Integer, Byte>>> userDataBroadcaster;

  private final Reduce.Sender<Map<Integer, Map<Integer, Byte>>> itemDataReducer;
  private final Broadcast.Receiver<Map<Integer, Map<Integer, Byte>>> itemDataBroadcaster;

  private Map<Integer, Map<Integer, Byte>> rowRates = null, colRates = null;

  private final RateList dataSet;

  @Inject
  SlaveTask(final RateList dataSet,
            final GroupCommClient groupCommClient) {
    this.dataSet = dataSet;
    this.communicationGroup = groupCommClient.getCommunicationGroup(ALSDriver.AllCommunicationGroup.class);
    this.controlMessageBroadcaster = communicationGroup.getBroadcastReceiver(ControlMessageBroadcaster.class);
    this.featureMatrixBroadcaster = communicationGroup.getBroadcastReceiver(FeatureBroadcaster.class);
    this.maxIndexReducer = communicationGroup.getReduceSender(MaxIndexReducer.class);
    this.userDataReducer = communicationGroup.getReduceSender(UserDataReducer.class);
    this.userDataBroadcaster = communicationGroup.getBroadcastReceiver(UserDataBroadcaster.class);

    this.itemDataReducer = communicationGroup.getReduceSender(UserDataReducer.class);
    this.itemDataBroadcaster = communicationGroup.getBroadcastReceiver(UserDataBroadcaster.class);
  }

  @Override
  public final byte[] call(final byte[] memento) throws Exception {
    loadData();

    for(boolean repeat = true; repeat; ) {

      final ControlMessages message = controlMessageBroadcaster.receive();
      switch (message) {
        // Get maximum indices for user and item data
        case GetMaxIndex:
          maxIndexReducer.send(new Pair<>(dataSet.getMaxUid(), dataSet.getMaxIid()));
          break;

        // Collect the data ordered by UserId
        case CollectUserData:
          userDataReducer.send(dataSet.getRowRate());
          dataSet.clearUserData(); // Clear the existing data to avoid redundancy
          break;

        // Re-distribute the user data
        case DistributeUserData:
          Map userData = userDataBroadcaster.receive();
          dataSet.addUserData(userData);
          break;

        // Collect the data ordered by ItemId
        case CollectItemData:
          userDataReducer.send(dataSet.getRowRate());
          dataSet.clearItemData(); // Clear the existing data to avoid redundancy
          break;

        // Re-distribute the user data
        case DistributeItemData:
          Map itemData = itemDataBroadcaster.receive();
          dataSet.addItemData(itemData);
          break;

        case Stop:
          LOG.info("Get STOP control massage. Terminate");
          repeat = false;
          break;
      }
    }

    return null;
  }

  private void loadData() {
    rowRates = dataSet.getRowRate();
    colRates = dataSet.getColRate();

    LOG.info("Loading data");
  }
}