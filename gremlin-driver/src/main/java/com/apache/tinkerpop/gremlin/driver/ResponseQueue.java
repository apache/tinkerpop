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
package com.apache.tinkerpop.gremlin.driver;

import com.apache.tinkerpop.gremlin.driver.message.ResponseMessage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A queue of incoming {@link ResponseMessage} objects.  The queue is updated by the
 * {@link com.apache.tinkerpop.gremlin.driver.Handler.GremlinResponseHandler} until a response terminator is identified.  At that point the fetch
 * status is changed to {@link Status#COMPLETE} and all results have made it client side.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class ResponseQueue {
    public enum Status {
        FETCHING,
        COMPLETE
    }

    private final LinkedBlockingQueue<ResponseMessage> responseQueue;

    private volatile Status status = Status.FETCHING;

    private final AtomicReference<Throwable> error = new AtomicReference<>();

    private final CompletableFuture<Void> readComplete;

    public ResponseQueue(final LinkedBlockingQueue<ResponseMessage> responseQueue, final CompletableFuture<Void> readComplete) {
        this.responseQueue = responseQueue;
        this.readComplete = readComplete;
    }

    public void add(final ResponseMessage msg) {
        this.responseQueue.offer(msg);
    }

    public int size() {
        if (error.get() != null) throw new RuntimeException(error.get());
        return this.responseQueue.size();
    }

    public boolean isEmpty() {
        if (error.get() != null) throw new RuntimeException(error.get());
        return this.size() == 0;
    }

    public ResponseMessage poll() {
        ResponseMessage msg = null;
        do {
            if (error.get() != null) throw new RuntimeException(error.get());
            try {
                msg = responseQueue.poll(10, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ie) {
                error.set(new RuntimeException(ie));
            }
        } while (null == msg && status == Status.FETCHING);

        if (error.get() != null) throw new RuntimeException(error.get());

        return msg;
    }

    public Status getStatus() {
        return status;
    }

    void markComplete() {
        this.status = Status.COMPLETE;
        this.readComplete.complete(null);
    }

    void markError(final Throwable throwable) {
        error.set(throwable);
        this.readComplete.complete(null);
    }
}
