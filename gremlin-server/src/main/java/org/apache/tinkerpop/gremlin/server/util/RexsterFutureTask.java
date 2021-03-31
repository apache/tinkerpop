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
package org.apache.tinkerpop.gremlin.server.util;

import org.apache.tinkerpop.gremlin.server.handler.Rexster;

import java.util.Optional;
import java.util.concurrent.FutureTask;

/**
 * A cancellable asynchronous operation with the added ability to get a {@code Rexster} instance if the
 * {@code Runnable} for the task was of that type.
 */
public class RexsterFutureTask<V> extends FutureTask<V> {

    private final Rexster rexster;

    public RexsterFutureTask(final Runnable runnable, final  V result) {
        super(runnable, result);

        // hold an instance to the Rexster instance if it is of that type
        this.rexster = runnable instanceof Rexster ? (Rexster) runnable : null;
    }

    public Optional<Rexster> getRexster() {
        return Optional.ofNullable(this.rexster);
    }
}
