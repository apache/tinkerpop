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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.exec;

/**
 * Feeds an {@code Input} source: the upstream traversers at the top level, a parent operator's current batch in a
 * child plan.
 */
@FunctionalInterface
public interface BatchSupplier {

    /**
     * Clears {@code out}, fills it with the next entries, at least one, of the lane and shape the pipeline was built for.
     *
     * @return false, with {@code out} empty, when there is no more input
     */
    boolean next(Batch out);
}
