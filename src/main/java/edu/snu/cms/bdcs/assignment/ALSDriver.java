package edu.snu.cms.bdcs.assignment;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ServiceConfiguration;
import com.microsoft.reef.driver.task.CompletedTask;
import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.evaluator.context.parameters.ContextIdentifier;
import com.microsoft.reef.io.data.loading.api.DataLoadingService;
import com.microsoft.reef.io.network.naming.NameServerParameters;
import com.microsoft.reef.io.network.nggroup.api.driver.CommunicationGroupDriver;
import com.microsoft.reef.io.network.nggroup.api.driver.GroupCommDriver;
import com.microsoft.reef.io.network.nggroup.impl.config.BroadcastOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.ReduceOperatorSpec;
import com.microsoft.reef.io.serialization.SerializableCodec;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Configurations;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationSerializer;
import com.microsoft.wake.EventHandler;
import edu.snu.cms.bdcs.assignment.data.Parser;
import edu.snu.cms.bdcs.assignment.data.RateList;
import edu.snu.cms.bdcs.assignment.data.ymusic.MusicDataParser;
import edu.snu.cms.bdcs.assignment.operators.*;
import edu.snu.cms.bdcs.assignment.operators.functions.*;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 */
@DriverSide
@Unit
public final class ALSDriver {

  private static final Logger LOG = Logger.getLogger(ALSDriver.class.getName());

  private final DataLoadingService dataLoadingService;
  private final GroupCommDriver groupCommDriver;
  private final ConfigurationSerializer confSerializer;
  private final CommunicationGroupDriver communicationsGroup;
  private final AtomicInteger slaveIds = new AtomicInteger(0);
  private final Map<String, RunningTask> runningTasks = new HashMap<>();
  private final AtomicBoolean jobComplete = new AtomicBoolean(false);
  private final AtomicBoolean masterSubmitted = new AtomicBoolean(false);

  private String communicationsGroupMasterContextId;

  private final int maxIter;
  private final int numFeat;

  /**
   */
  @Inject
  public ALSDriver(final DataLoadingService dataLoadingService,
                   final GroupCommDriver groupCommDriver,
                   final ConfigurationSerializer confSerializer,
                   final @Parameter(ALS.MaxIter.class) int maxIter,
                   final @Parameter(ALS.NumFeature.class) int numFeat) {
    LOG.log(Level.FINE, "Instantiated 'ALSDriver'");

    this.maxIter = maxIter;
    this.numFeat = numFeat;

    this.dataLoadingService = dataLoadingService;
    this.groupCommDriver = groupCommDriver;
    this.confSerializer = confSerializer;

    final int numPartition = dataLoadingService.getNumberOfPartitions();
    final int numParticipants = numPartition + 1;
    this.communicationsGroup = this.groupCommDriver.newCommunicationGroup(
      AllCommunicationGroup.class,
      numParticipants);
    LOG.log(Level.INFO, "Obtained entire communication group: start with {0} partitions", numParticipants);

    this.communicationsGroup
      .addBroadcast(ControlMessageBroadcaster.class,
        BroadcastOperatorSpec.newBuilder()
          .setSenderId(MasterTask.TASK_ID)
          .setDataCodecClass(SerializableCodec.class)
          .build()) // For Control message communication
      .addReduce(MaxIndexReducer.class,
        ReduceOperatorSpec.newBuilder()
          .setReceiverId(MasterTask.TASK_ID)
          .setDataCodecClass(SerializableCodec.class)
          .setReduceFunctionClass(MaxIndexReduceFunction.class)
          .build()) // For getting Maximum indices. Assume all the indices are normalized to start at 1
            .addBroadcast(MaxIndexBroadcaster.class,
        BroadcastOperatorSpec.newBuilder()
          .setSenderId(MasterTask.TASK_ID)
          .setDataCodecClass(SerializableCodec.class)
          .build()) // For Control message communication
      .addReduce(UserDataReducer.class,
        ReduceOperatorSpec.newBuilder()
          .setReceiverId(MasterTask.TASK_ID)
          .setDataCodecClass(SerializableCodec.class)
          .setReduceFunctionClass(UserDataReduceFunction.class)
          .build()) // To collect data to group by user id.
      .addBroadcast(UserDataBroadcaster.class,
        BroadcastOperatorSpec.newBuilder()
          .setSenderId(MasterTask.TASK_ID)
          .setDataCodecClass(SerializableCodec.class)
          .build()) // To distribute the data grouped by user id.
      .addReduce(ItemDataReducer.class,
        ReduceOperatorSpec.newBuilder()
          .setReceiverId(MasterTask.TASK_ID)
          .setDataCodecClass(SerializableCodec.class)
          .setReduceFunctionClass(ItemDataReduceFunction.class)
          .build()) // To collect data to group by item id.
      .addBroadcast(ItemDataBroadcaster.class,
        BroadcastOperatorSpec.newBuilder()
          .setSenderId(MasterTask.TASK_ID)
          .setDataCodecClass(SerializableCodec.class)
          .build()) // To distribute the data grouped by item id.
      .addBroadcast(FeatureMatrixBroadcaster.class,
        BroadcastOperatorSpec.newBuilder()
          .setSenderId(MasterTask.TASK_ID)
          .setDataCodecClass(SerializableCodec.class)
          .build()) // To broadcast the Feature matrix
      .addReduce(FeatureMatrixReducer.class,
        ReduceOperatorSpec.newBuilder()
          .setReceiverId(MasterTask.TASK_ID)
          .setDataCodecClass(SerializableCodec.class)
          .setReduceFunctionClass(FeatureMatrixReduceFunction.class)
          .build()) // For Feature matrix broadcast
      .finalise();
  }

  /**
   * Handles ActiveContext event
   */
  public final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(ActiveContext activeContext) {
      LOG.log(Level.INFO, "Got active context: {0}", activeContext.getId());
      if(jobRunning(activeContext)) {
        if (!groupCommDriver.isConfigured(activeContext)) {
          // The Context is not configured with the group communications service let's do that.
          submitGroupCommunicationsService(activeContext);
        }
        else {
          // The group communications service is already active on this context. We can submit the task.
          submitTask(activeContext);
        }
      }
    }
  }

  private void submitGroupCommunicationsService(final ActiveContext activeContext) {
    final Configuration contextConf = groupCommDriver.getContextConfiguration();
    final String contextId = getContextId(contextConf);
    final Configuration serviceConf;
    if (!dataLoadingService.isDataLoadedContext(activeContext)) {
      communicationsGroupMasterContextId = contextId;
      serviceConf = groupCommDriver.getServiceConfiguration();
    } else {
      final Configuration parsedDataServiceConf = ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, RateList.class)
        .build();
      serviceConf = Tang.Factory.getTang()
        .newConfigurationBuilder(groupCommDriver.getServiceConfiguration(), parsedDataServiceConf)
        .bindImplementation(Parser.class, MusicDataParser.class)
        .build();
    }

    LOG.log(Level.FINE, "Submit GCContext conf: {0} and Service conf: {1}", new Object[] {
      confSerializer.toString(contextConf), confSerializer.toString(serviceConf) });

    activeContext.submitContextAndService(contextConf, serviceConf);
  }

  private void submitTask(final ActiveContext activeContext) {
    assert (groupCommDriver.isConfigured(activeContext));

    final Configuration partialTaskConfiguration;
    if (activeContext.getId().equals(communicationsGroupMasterContextId) && !masterTaskSubmitted()) {
      partialTaskConfiguration = getMasterTaskConfiguration();
      LOG.info("Submitting MasterTask conf");
    } else {
      partialTaskConfiguration = Configurations.merge(
        getSlaveTaskConfiguration(getSlaveId(activeContext)));
      //, getTaskPoisonConfiguration());
      LOG.info("Submitting SlaveTask conf");
    }
    communicationsGroup.addTask(partialTaskConfiguration);
    final Configuration taskConfiguration = groupCommDriver.getTaskConfiguration(partialTaskConfiguration);
    LOG.log(Level.FINEST, "{0}", confSerializer.toString(taskConfiguration));
    activeContext.submitTask(taskConfiguration);
  }



  final class TaskRunningHandler implements EventHandler<RunningTask> {

    @Override
    public void onNext(final RunningTask runningTask) {
      synchronized (runningTasks) {
        if (!jobComplete.get()) {
          LOG.log(Level.INFO, "Job has not completed yet. Adding to runningTasks: {0}", runningTask);
          runningTasks.put(runningTask.getId(), runningTask);
        } else {
          LOG.log(Level.INFO, "Job complete. Closing context: {0}", runningTask.getActiveContext().getId());
          runningTask.getActiveContext().close();
        }
      }
    }
  }

  final class TaskFailedHandler implements EventHandler<FailedTask> {

    @Override
    public void onNext(final FailedTask failedTask) {
      final String failedTaskId = failedTask.getId();
      LOG.log(Level.WARNING, "Got failed Task: " + failedTaskId);//, failedTask.asError());

      if (jobRunning(failedTaskId)) {

        final ActiveContext activeContext = failedTask.getActiveContext().get();
        final Configuration partialTaskConf = getSlaveTaskConfiguration(failedTaskId);

        // Do not add the task back:
        // allCommGroup.addTask(partialTaskConf);

        final Configuration taskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
        LOG.log(Level.FINEST, "Submit SlaveTask conf: {0}", confSerializer.toString(taskConf));

        activeContext.submitTask(taskConf);
      }
    }
  }

  final class TaskCompletedHandler implements EventHandler<CompletedTask> {

    @Override
    public void onNext(final CompletedTask task) {

      LOG.log(Level.INFO, "Got CompletedTask: {0}", task.getId());
      final byte[] retVal = task.get();
      if (retVal != null) {
        LOG.log(Level.INFO, "The result of Task is " + new String(retVal));
      }

      synchronized (runningTasks) {
        LOG.log(Level.INFO, "Acquired lock on runningTasks. Removing {0}", task.getId());
        final RunningTask rTask = runningTasks.remove(task.getId());
        if (rTask != null) {
          LOG.log(Level.INFO, "Closing active context: {0}", task.getActiveContext().getId());
          task.getActiveContext().close();
        } else {
          LOG.log(Level.INFO, "Master must have closed active context already for task {0}", task.getId());
        }

        if (MasterTask.TASK_ID.equals(task.getId())) {
          jobComplete.set(true);
          LOG.log(Level.INFO, "Master(=>Job) complete. Closing other running tasks: {0}", runningTasks.values());
          for (final RunningTask runTask : runningTasks.values()) {
            LOG.log(Level.INFO, "Master tyring to close {0}", runTask);
            runTask.getActiveContext().close();
          }
          LOG.finest("Clearing runningTasks");
          runningTasks.clear();
        }
      }
    }
  }

  /**
   * @param activeContext
   * @return
   */
  private boolean jobRunning(final ActiveContext activeContext) {
    synchronized (runningTasks) {
      if (!jobComplete.get()) {
        return true;
      } else {
        LOG.log(Level.INFO, "Job complete. Not submitting any task. Closing context {0}", activeContext);
        activeContext.close();
        return false;
      }
    }
  }

  public boolean jobRunning(final String taskId) {
    synchronized (runningTasks) {
      if (!jobComplete.get()) {
        return true;
      } else {
        final RunningTask rTask = runningTasks.remove(taskId);
        LOG.log(Level.INFO, "Job has completed. Not resubmitting");
        if (rTask != null) {
          LOG.log(Level.INFO, "Closing activecontext");
          rTask.getActiveContext().close();
        }
        else {
          LOG.log(Level.INFO, "Master must have closed my context");
        }
        return false;
      }
    }
  }
  /**
   * @return Configuration for the MasterTask
   */
  public Configuration getMasterTaskConfiguration() {
    return Tang.Factory.getTang()
      .newConfigurationBuilder(TaskConfiguration.CONF
        .set(TaskConfiguration.IDENTIFIER, MasterTask.TASK_ID)
        .set(TaskConfiguration.TASK, MasterTask.class)
        .build())
      .bindNamedParameter(ALS.NumFeature.class, String.valueOf(numFeat))
      .build();
  }

  /**
   * @return Configuration for the SlaveTask
   */
  private Configuration getSlaveTaskConfiguration(final String taskId) {
    return Tang.Factory.getTang()
      .newConfigurationBuilder(
        TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, taskId)
          .set(TaskConfiguration.TASK, SlaveTask.class)
          .build())
      .bindNamedParameter(ALS.TaskIndex.class, String.valueOf(slaveIds.get()))
      .bindNamedParameter(ALS.MaxIter.class, String.valueOf(maxIter))
      .build();
  }

  private String getContextId(final Configuration contextConf) {
    try {
      return Tang.Factory.getTang().newInjector(contextConf).getNamedInstance(ContextIdentifier.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("Unable to inject context identifier from context conf", e);
    }
  }

  private String getSlaveId(final ActiveContext activeContext) {
    return "SlaveTask-" + slaveIds.getAndIncrement();
  }

  private boolean masterTaskSubmitted() {
    return !masterSubmitted.compareAndSet(false, true);
  }

  /**
   */
  @NamedParameter
  public static class AllCommunicationGroup implements Name<String> {
  }
}