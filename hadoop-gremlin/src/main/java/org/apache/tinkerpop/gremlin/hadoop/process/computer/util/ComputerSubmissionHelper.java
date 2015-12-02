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
package org.apache.tinkerpop.gremlin.hadoop.process.computer.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

import com.google.common.base.Function;

import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;

public class ComputerSubmissionHelper {

    /**
     * Creates a {@link Executors#newSingleThreadExecutor(ThreadFactory)} configured
     * make threads that behave like the caller, invokes a closure on it, and shuts it down.
     * <p>
     * This is intended to serve as an alternative to {@link ForkJoinPool#commonPool()},
     * which is used by {@link CompletableFuture#supplyAsync(Supplier)} (among other methods).
     * The the single threaded executor created by this method contains a thread
     * with the same context classloader and thread group as the thread that called
     * this method.  Threads created in this method also have predictable behavior when
     * {@link Thread#setContextClassLoader(ClassLoader)} is invoked; threads in the
     * common pool throw a SecurityException if the JVM has a security manager configured.
     * <p>
     * The name of the thread created by this method's internal executor is the concatenation of
     * <ul>
     *     <li>the name of the thread that calls this method</li>
     *     <li>"-TP-"</li>
     *     <li>the {@code threadNameSuffix} parameter value</li>
     * </ul>
     *
     * @param closure arbitrary code that has exclusive use of the supplied executor
     * @param threadNameSuffix a string appended to the executor's thread's name
     * @return the return value of the {@code closure} parameter
     */
    public static Future<ComputerResult> runWithBackgroundThread(Function<Executor, Future<ComputerResult>> closure,
                                                                 String threadNameSuffix) {
        final Thread callingThread = Thread.currentThread();
        final ClassLoader classLoader = callingThread.getContextClassLoader();
        final ThreadGroup threadGroup = callingThread.getThreadGroup();
        final String threadName = callingThread.getName();
        ExecutorService submissionExecutor = null;

        try {
            submissionExecutor = Executors.newSingleThreadExecutor(runnable -> {
                Thread t = new Thread(threadGroup, runnable, threadName + "-TP-" + threadNameSuffix);
                t.setContextClassLoader(classLoader);
                return t;
            });

            return closure.apply(submissionExecutor);
        } finally {
            if (null != submissionExecutor)
                submissionExecutor.shutdown(); // do not call shutdownNow, which could prematurely terminate the closure
        }
    }
}
