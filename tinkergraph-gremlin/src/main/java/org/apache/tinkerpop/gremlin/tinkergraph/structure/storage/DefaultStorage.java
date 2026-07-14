/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.structure.storage;

import java.util.function.Supplier;

/**
 * The built-in {@link TinkerStorage} engines that can be selected by name via the
 * {@code gremlin.tinkergraph.storage} configuration key. A fully-qualified class name may be used instead of one of
 * these names to plug in a custom engine.
 */
public enum DefaultStorage implements Supplier<TinkerStorage> {

    /**
     * A durable, append-only commit log ("write-ahead log") that serializes each committed transaction with
     * GraphBinary. See {@link GraphBinaryStorage}.
     */
    GRAPHBINARY {
        @Override
        public TinkerStorage get() {
            return new GraphBinaryStorage();
        }
    }
}
