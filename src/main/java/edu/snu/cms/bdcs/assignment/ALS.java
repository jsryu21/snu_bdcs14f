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

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.io.data.loading.api.DataLoadingRequestBuilder;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.CommandLine;
import org.apache.hadoop.mapred.TextInputFormat;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of Alternating Least Squares(ALS)
 * algorithm for Collaborative Filtering.
 * TODO elaborate the detail of application workflow
 */
public final class ALS {

  private static final Logger LOG = Logger.getLogger(ALS.class.getName());


  private static final int NUM_LOCAL_THREADS = 16;
  private static final int NUM_SPLITS = 6;
  private static final int NUM_COMPUTE_EVALUATORS = 2;

  @NamedParameter(doc = "Whether or not to run on the local runtime",
    short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {
  }

  @NamedParameter(doc = "Number of minutes before timeout",
    short_name = "timeout", default_value = "2")
  public static final class TimeOut implements Name<Integer> {
  }

  @NamedParameter(doc = "The path where input data files exist",
    short_name = "input")
  public static final class InputDir implements Name<String> {
  }

  @NamedParameter(doc = "Number of feature to approximate",
    short_name = "num_feat", default_value = "3")
  public static final class NumFeatures implements Name<String> {
  }

  @NamedParameter(doc = "The coefficient term for regularization",
    short_name = "lambda", default_value = "0.01")
  public static final class Lambda implements Name<String> {
  }

  /**
   *
   * @param args command line parameters.
   * @throws com.microsoft.tang.exceptions.BindException      configuration error.
   * @throws com.microsoft.tang.exceptions.InjectionException configuration error.
   */
  public static void main(final String[] args) throws BindException, InjectionException, IOException {
    final Tang tang = Tang.Factory.getTang();

    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();

    new CommandLine(cb)
      .registerShortNameOfClass(Local.class)
      .registerShortNameOfClass(TimeOut.class)
      .registerShortNameOfClass(InputDir.class)
      .registerShortNameOfClass(NumFeatures.class)
      .registerShortNameOfClass(Lambda.class)
      .processCommandLine(args);

    final Injector injector = tang.newInjector(cb.build());

    final boolean isLocal = injector.getNamedInstance(Local.class);
    final int jobTimeout = injector.getNamedInstance(TimeOut.class) * 60 * 1000;
    final String inputDir = injector.getNamedInstance(InputDir.class);
    final Configuration runtimeConfiguration;
    if (isLocal) {
      LOG.log(Level.INFO, "Running Data Loading demo on the local runtime");
      runtimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, NUM_LOCAL_THREADS)
        .build();
    } else {
      LOG.log(Level.INFO, "Running Data Loading demo on YARN");
      runtimeConfiguration = YarnClientConfiguration.CONF.build();
    }

    final EvaluatorRequest computeRequest = EvaluatorRequest.newBuilder()
      .setNumber(NUM_COMPUTE_EVALUATORS)
      .setMemory(512)
      .setNumber(1)
      .build();

    final Configuration driverConfiguration =
      new DataLoadingRequestBuilder()
        .setMemoryMB(1024)
        .setComputeRequest(computeRequest)
        .setInputFormatClass(TextInputFormat.class)
        .setInputPath(inputDir)
        .setNumberOfDesiredSplits(NUM_SPLITS)
        .setDriverConfigurationModule(DriverConfiguration.CONF
            .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(ALSDriver.class))
            .set(DriverConfiguration.DRIVER_IDENTIFIER, "ALS")
            .set(DriverConfiguration.ON_CONTEXT_ACTIVE, ALSDriver.ActiveContextHandler.class)
            .set(DriverConfiguration.ON_TASK_COMPLETED, ALSDriver.TaskCompletedHandler.class)
        )
        .build();

    DriverLauncher.getLauncher(runtimeConfiguration).run(driverConfiguration, jobTimeout);
  }
}