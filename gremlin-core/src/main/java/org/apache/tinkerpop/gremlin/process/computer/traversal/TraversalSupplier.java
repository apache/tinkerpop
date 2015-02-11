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
package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.Traversal;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalSupplier<S, E> implements Supplier<Traversal.Admin<S, E>>, Serializable {

    private final Traversal.Admin<S, E> traversal;

    public TraversalSupplier(final Traversal.Admin<S, E> traversal) {
        this.traversal = traversal;
    }

    @Override
    public Traversal.Admin<S, E> get() {
        return this.traversal;
    }
}
