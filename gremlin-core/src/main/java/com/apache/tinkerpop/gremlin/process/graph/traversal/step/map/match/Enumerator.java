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
package com.apache.tinkerpop.gremlin.process.graph.traversal.step.map.match;

import java.util.function.BiConsumer;

/**
 * An array of key/value maps accessible by index.
 * The total size of the enumerator may be unknown when it is created;
 * it grows when successive solutions are requested, computing as many as necessary.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public interface Enumerator<T> {

    /**
     * @return the number of solutions so far known; the enumerator has at least this many.
     * This should be a relatively cheap operation.
     */
    int size();

    /**
     * Provides access to a solution, allowing it to be printed, put into a map, etc.
     *
     * @param index   the index of the solution
     * @param visitor a consumer for each key/value pair in the solution
     * @return whether a solution exists at the given index and was visited
     */
    boolean visitSolution(int index, BiConsumer<String, T> visitor);
}
