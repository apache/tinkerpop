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
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MemoryAccumulator<A> implements AccumulatorParam<ObjectWritable<A>> {

    private final MemoryComputeKey<A> memoryComputeKey;

    public MemoryAccumulator(final MemoryComputeKey<A> memoryComputeKey) {
        this.memoryComputeKey = memoryComputeKey;
    }

    @Override
    public ObjectWritable<A> addAccumulator(final ObjectWritable<A> a, final ObjectWritable<A> b) {
        if (a.isEmpty())
            return b;
        if (b.isEmpty())
            return a;
        return new ObjectWritable<>(this.memoryComputeKey.getReducer().apply(a.get(), b.get()));
    }

    @Override
    public ObjectWritable<A> addInPlace(final ObjectWritable<A> a, final ObjectWritable<A> b) {
        return this.addAccumulator(a, b);
    }

    @Override
    public ObjectWritable<A> zero(final ObjectWritable<A> a) {
        return ObjectWritable.empty();
    }
}
