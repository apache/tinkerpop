/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.driver;

import org.junit.Before;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractResultQueueTest {
    protected final ExecutorService pool = Executors.newCachedThreadPool();
    protected ResultQueue resultQueue;
    protected CompletableFuture<Void> readCompleted;

    @Before
    public void setup() {
        final LinkedBlockingQueue<Result> resultLinkedBlockingQueue = new LinkedBlockingQueue<>();
        readCompleted = new CompletableFuture<>();
        resultQueue = new ResultQueue(resultLinkedBlockingQueue, readCompleted);
    }

    protected Thread addToQueue(final int numberOfItemsToAdd, final long pauseBetweenItemsInMillis,
                                final boolean start) throws Exception {
        return addToQueue(numberOfItemsToAdd, pauseBetweenItemsInMillis, start, false);
    }

    protected Thread addToQueue(final int numberOfItemsToAdd, final long pauseBetweenItemsInMillis,
                                final boolean start, final boolean markDone) throws Exception {
        final Thread t = new Thread(() -> {
            boolean done = false;
            for (int ix = 0; ix < numberOfItemsToAdd && !done; ix++) {
                try {
                    if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
                    final int currentIndex = ix;
                    pool.submit(() -> {
                        final Result result = new Result("test-" + currentIndex);
                        resultQueue.add(result);
                    }).get();
                    TimeUnit.MILLISECONDS.sleep(pauseBetweenItemsInMillis);
                } catch (Exception ie) {
                    done = true;
                }
            }

            if (markDone) resultQueue.markComplete();

        }, "ResultQueueTest-job-submitter");

        if (start) t.start();

        return t;
    }
}
