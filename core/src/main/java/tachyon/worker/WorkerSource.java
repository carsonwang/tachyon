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

package tachyon.worker;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import tachyon.metrics.source.Source;

/**
 * A WorkerSource collects a Worker's internal state.
 */
public class WorkerSource implements Source {
  private MetricRegistry mMetricRegistry = new MetricRegistry();
  private final Counter mBlocksAccessed = mMetricRegistry.counter(MetricRegistry
          .name("BlocksAccessed"));
  private final Counter mBlocksCanceled = mMetricRegistry.counter(MetricRegistry
          .name("BlocksCanceled"));
  private final Counter mBlocksDeleted = mMetricRegistry.counter(MetricRegistry
          .name("BlocksDeleted"));
  private final Counter mBlocksEvicted = mMetricRegistry.counter(MetricRegistry
          .name("BlocksEvicted"));
  private final Counter mBlocksPromoted = mMetricRegistry.counter(MetricRegistry
          .name("BlocksPromoted"));
  // metrics from client
  private final Counter mBlocksReadLocal = mMetricRegistry.counter(MetricRegistry
          .name("BlocksReadLocal"));
  private final Counter mBlocksReadRemote = mMetricRegistry.counter(MetricRegistry
          .name("BlocksReadRemote"));
  private final Counter mBlocksWritten = mMetricRegistry.counter(MetricRegistry
          .name("BlocksWritten"));
  private final Counter mBytesReadLocal = mMetricRegistry.counter(MetricRegistry
          .name("BytesReadLocal"));
  private final Counter mBytesReadRemote = mMetricRegistry.counter(MetricRegistry
          .name("BytesReadRemote"));
  private final Counter mBytesReadUfs = mMetricRegistry.counter(MetricRegistry
          .name("BytesReadUfs"));
  private final Counter mBytesWritten = mMetricRegistry.counter(MetricRegistry
          .name("BytesWritten"));


  public WorkerSource(final WorkerStorage workerStorage) {
    mMetricRegistry.register(MetricRegistry.name("CapacityTotalGB"), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return workerStorage.getCapacityBytes();
      }
    });

    mMetricRegistry.register(MetricRegistry.name("CapacityUsedGB"), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return workerStorage.getUsedBytes();
      }
    });

    mMetricRegistry.register(MetricRegistry.name("CapacityFreeGB"), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return workerStorage.getCapacityBytes() - workerStorage.getUsedBytes();
      }
    });

    mMetricRegistry.register(MetricRegistry.name("BlocksCached"), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return workerStorage.getNumberOfBlocks();
      }
    });
  }

  @Override
  public String getName() {
    return "worker";
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return mMetricRegistry;
  }

  public void incBlocksAccessed() {
    mBlocksAccessed.inc();
  }

  public void incBlocksCanceled() {
    mBlocksCanceled.inc();
  }

  public void incBlocksDeleted() {
    mBlocksDeleted.inc();
  }

  public void incBlocksEvicted() {
    mBlocksEvicted.inc();
  }

  public void incBlocksPromoted() {
    mBlocksPromoted.inc();
  }

  public void incBlocksReadLocal() {
    mBlocksReadLocal.inc();
  }

  public void incBlocksReadRemote() {
    mBlocksReadRemote.inc();
  }

  public void incBlocksWritten() {
    mBlocksWritten.inc();
  }

  public void incBytesReadLocal(long n) {
    mBytesReadLocal.inc(n);
  }

  public void incBytesReadRemote(long n) {
    mBytesReadRemote.inc(n);
  }

  public void incBytesReadUfs(long n) {
    mBytesReadUfs.inc(n);
  }

  public void incBytesWritten(long n) {
    mBytesWritten.inc(n);
  }

}
