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
import org.apache.mahout.math.*;

import javax.inject.Inject;
import java.util.HashMap;
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
  private final Broadcast.Receiver<Pair<Integer, Pair<Integer, Integer>>> maxIndexBroadcaster;

  private final Reduce.Sender<Map<Integer, Map<Integer, Byte>>> userDataReducer;
  private final Broadcast.Receiver<Map<Integer, Map<Integer, Byte>>> userDataBroadcaster;

  private final Reduce.Sender<Map<Integer, Map<Integer, Byte>>> itemDataReducer;
  private final Broadcast.Receiver<Map<Integer, Map<Integer, Byte>>> itemDataBroadcaster;

  private final Broadcast.Receiver<Map<Integer, Map<Integer, Double>>> featureMatrixBroadcaster;
  private final Reduce.Sender<Map<Integer, Map<Integer, Double>>> featureMatrixReducer;

  private Map<Integer, Map<Integer, Byte>> rowRates = null, colRates = null;

  private final RateList dataSet;
  private final int numFeat;
  private final double lambda = 0.3;

  /**
   * task index to distribute the computation and data holding
   */
  private final int taskId;

  private int totalTask = 0;
  private int totalUser = 0;
  private int totalItem = 0;


  Map<Integer, Map<Integer, Double>> itemMatrix = null;
  Map<Integer, Map<Integer, Double>> userMatrix = null;

  @Inject
  SlaveTask(final RateList dataSet,
            final GroupCommClient groupCommClient,
            final @Parameter(ALS.TaskIndex.class) int taskId,
            final @Parameter(ALS.NumFeature.class) int numFeat) {
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
    this.numFeat = numFeat;
    LOG.info("This is slave Task with ID"+ taskId);
  }

  @Override
  public final byte[] call(final byte[] memento) throws Exception {
    loadData();

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
          final Pair<Integer, Pair<Integer, Integer>> maxIdPResolved = maxIndexBroadcaster.receive();
          totalTask = maxIdPResolved.first;
          totalUser = maxIdPResolved.second.first;
          totalItem = maxIdPResolved.second.second;

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
          userMatrix = computeU();
          featureMatrixReducer.send(userMatrix); // TODO originally should send the user matrix
          break;

        case CollectItemFeatureMatrix:
          LOG.info("Collect the feature matrix of Item(M)");
          itemMatrix = computeM();
          featureMatrixReducer.send(itemMatrix);
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


  private Map<Integer, Map<Integer, Double>> computeM() {
   Map itemMatrix = new HashMap<Integer, Map<Integer, Double>>();
    for (int i=0; i<totalItem; i++) {
      Matrix UIi = getMIi(i, totalUser);
      itemMatrix.put(i, convertVectorToMap(getUi(getAi(rowRates, UIi, lambda, i), getVi(UIi, i, totalUser))));
    }
    return itemMatrix;
  }

  private Map<Integer, Map<Integer, Double>> computeU() {
    Map userMatrix = new HashMap<Integer, Map<Integer, Double>>();
    for (int i=0; i<totalUser; i++) {
      Matrix MIi = getMIi(i, totalItem);
      userMatrix.put(i, convertVectorToMap(getUi(getAi(colRates, MIi, lambda, i), getVi(MIi, i, totalItem))));
    }
    return userMatrix;
  }

  private Matrix getMIi(int i, int jMax) {
    // Update Mi with Ui and given M(itemMatrix)
    Map<Integer, Byte> Ui = rowRates.get(i);
    Matrix result = new DenseMatrix(numFeat, jMax);

    for(int k = 0; k < numFeat; k++) {
      for(int j = 0; j < jMax; j++) {
        if (Ui != null && Ui.containsKey(j))
          result.set(k, j, itemMatrix.get(i).get(j));
        else
          result.set(k, j, 0);
      }
    }
    return result;
  }

  private Matrix getAi(Map<Integer, Map<Integer, Byte>> R, Matrix MIi, double lambda, int i) {
    int nUi = R.get(i).keySet().size(); // nUi : the number of items User i rated
    Matrix E = DiagonalMatrix.identity(nUi).times(lambda);
    Matrix Ai = MIi.times(MIi.transpose()).plus(E); // Ai : Mi * Mi_t + lambda * NUi * E
    return Ai;
  }

  private Matrix getVi(Matrix MIi, int i, int jMax) {
    Matrix RiIi = new DenseMatrix(1, jMax);
    for (int j=0; j<jMax; j++) {
      if(rowRates.get(i).containsKey(j))
        RiIi.set(1, j, rowRates.get(i).get(j));
    }
    Matrix Vi = MIi.times(RiIi.transpose()); // Vi = Mi*R_tIi
    return Vi;
  }

  private Vector getUi(Matrix Ai, Matrix Vj) {
    return new QRDecomposition(Ai).solve(Vj).viewColumn(0); // Ui = Ai^-1*Vj
  }

  /**
   * Convert Matrix to Map
   * @param m
   * @return
   */
  private Map<Integer, Map<Integer, Double>> convertMatToMap(final Matrix m) {
    Map<Integer, Map<Integer, Double>> result = new HashMap<>();
    for (int i = 0; i < m.rowSize(); i++) {
      if (!result.containsKey(i))
        result.put(i, new HashMap<Integer, Double>());
      for (int j = 0; j < m.columnSize(); j++) {
        result.get(i).put(j, m.get(i, j));
      }
    }
    return result;
  }

  /**
   * Convert Vector to Map to enable Serialization
   * @param u
   * @return
   */
  private Map<Integer, Double> convertVectorToMap(final Vector u) {
    Map<Integer, Double> result = new HashMap<>();
    for (int i = 0; i < u.size(); i++) {
      result.put(i, u.get(i));
    }
    return result;
  }
}