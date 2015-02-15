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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;

import tachyon.Constants;
import tachyon.conf.CommonConf;
import tachyon.metrics.sink.Sink;
import tachyon.metrics.source.Source;


public class MetricsSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final String SINK_REGEX = "^sink\\.(.+)\\.(.+)";
  private static final String SOURCE_REGEX = "^source\\.(.+)\\.(.+)";
  private static final TimeUnit MINIMAL_POLL_UNIT = TimeUnit.SECONDS;
  private static final int MINIMAL_POLL_PERIOD = 1;

  private String mInstance;
  private List<Sink> mSinks = new ArrayList<Sink>();
  private List<Source> mSources = new ArrayList<Source>();
  private MetricRegistry mMetricRegistry = new MetricRegistry();
  private MetricsConfig mMetricsConfig;
  private boolean mRunning = false;

  public static void checkMinimalPollingPeriod(TimeUnit pollUnit, int pollPeriod)
      throws IllegalArgumentException {
    int period = (int) MINIMAL_POLL_UNIT.convert(pollPeriod, pollUnit);
    if (period < MINIMAL_POLL_PERIOD) {
      throw new IllegalArgumentException("Polling period " + pollPeriod + " " + pollUnit
          + " is below than minimal polling period");
    }
  }

  public MetricsSystem(String instance) {
    mInstance = instance;
    mMetricsConfig = new MetricsConfig(CommonConf.get().METRICS_CONF_FILE);
    mMetricsConfig.initialize();
  }

  public void registerSource(Source source) {
    mSources.add(source);
    try {
      mMetricRegistry.register(source.getName(), source.getMetricRegistry());
    } catch (IllegalArgumentException e) {
      LOG.info("Metrics already registered", e);
    }
  }

  private void registerSources() {
    Properties instConfig = mMetricsConfig.getInstance(mInstance);
    Map<String, Properties> sourceConfigs = mMetricsConfig.subProperties(instConfig, SOURCE_REGEX);
    for (Map.Entry<String, Properties> entry : sourceConfigs.entrySet()) {
      String classPath = entry.getValue().getProperty("class");
      if (classPath != null) {
        try {
          Source source = (Source) Class.forName(classPath).newInstance();
          registerSource(source);
        } catch (Exception e) {
          LOG.error("Source class {} cannot be instantiated", classPath, e);
        }
      }
    }
  }

  private void registerSinks() {
    Properties instConfig = mMetricsConfig.getInstance(mInstance);
    Map<String, Properties> sinkConfigs = mMetricsConfig.subProperties(instConfig, SINK_REGEX);
    for (Map.Entry<String, Properties> entry : sinkConfigs.entrySet()) {
      String classPath = entry.getValue().getProperty("class");
      if (classPath != null) {
        try {
          Sink sink =
              (Sink) Class.forName(classPath)
                  .getConstructor(Properties.class, MetricRegistry.class)
                  .newInstance(entry.getValue(), mMetricRegistry);
          mSinks.add(sink);
        } catch (Exception e) {
          LOG.error("Sink class {} cannot be instantiated", classPath, e);
        }
      }
    }
  }

  public void removeSource(Source source) {
    mSources.remove(source);
    mMetricRegistry.remove(source.getName());
  }

  public void report() {
    for (Sink sink : mSinks) {
      sink.report();
    }
  }

  public void start() {
    if (!mRunning) {
      registerSources();
      registerSinks();
      for (Sink sink : mSinks) {
        sink.start();
      }
      mRunning = true;
    } else {
      LOG.warn("Attempting to start a MetricsSystem that is already running");
    }
  }

  public void stop() {
    if (mRunning) {
      for (Sink sink : mSinks) {
        sink.stop();
      }
      mRunning = false;
    } else {
      LOG.warn("Stopping a MetricsSystem that is not running");
    }
  }
}
