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

package org.apache.tinkerpop.gremlin.spark.process.computer;

import org.apache.spark.AccumulatorParam;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MemoryAccumulator<A> implements AccumulatorParam<A> {

    private final MemoryComputeKey<A> memoryComputeKey;

    public MemoryAccumulator(final MemoryComputeKey<A> memoryComputeKey) {
        this.memoryComputeKey = memoryComputeKey;
    }

    @Override
    public A addAccumulator(final A a, final A b) {
        return null == a ? b : this.memoryComputeKey.getReducer().apply(a, b);
    }

    @Override
    public A addInPlace(final A a, final A b) {
        return null == a ? b : this.memoryComputeKey.getReducer().apply(a, b);
    }

    @Override
    public A zero(final A a) {
        return null;
    }
}
