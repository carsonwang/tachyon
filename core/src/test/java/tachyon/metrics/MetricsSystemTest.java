/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.metrics;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;
import tachyon.conf.CommonConf;
import tachyon.master.MasterSource;
import tachyon.worker.WorkerSource;

public class MetricsSystemTest {
  private String mFilePath;

  @After
  public final void After() {
    System.clearProperty("tachyon.metrics.conf");
  }

  @Before
  public final void Before() {
    mFilePath = getClass().getClassLoader().getResource("test_metrics.properties").getFile();
    System.setProperty("tachyon.metrics.conf", mFilePath);
    CommonConf.clear();
  }

  @Test
  public void metricsSystemTest() {
    MetricsSystem masterMetricsSystem = new MetricsSystem("master");
    masterMetricsSystem.start();

    Assert.assertEquals(2, masterMetricsSystem.getSinks().size());
    Assert.assertEquals(1, masterMetricsSystem.getSources().size());
    masterMetricsSystem.registerSource(new MasterSource(null));
    Assert.assertEquals(2, masterMetricsSystem.getSources().size());
    masterMetricsSystem.stop();

    MetricsSystem workerMetricsSystem = new MetricsSystem("worker");
    workerMetricsSystem.start();

    Assert.assertEquals(1, workerMetricsSystem.getSinks().size());
    Assert.assertEquals(1, workerMetricsSystem.getSources().size());
    workerMetricsSystem.registerSource(new WorkerSource(null));
    Assert.assertEquals(2, workerMetricsSystem.getSources().size());
    workerMetricsSystem.stop();
  }
}
