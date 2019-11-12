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

import org.apache.spark.util.AccumulatorV2;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class MemoryAccumulator<A> extends AccumulatorV2<ObjectWritable<A>, ObjectWritable<A>> {

    private final MemoryComputeKey<A> memoryComputeKey;
    private ObjectWritable<A> value;

    MemoryAccumulator(final MemoryComputeKey<A> memoryComputeKey) {
        this(memoryComputeKey, ObjectWritable.empty());
    }

    private MemoryAccumulator(final MemoryComputeKey<A> memoryComputeKey, final ObjectWritable<A> initial) {
        this.memoryComputeKey = memoryComputeKey;
        this.value = initial;
    }

    @Override
    public boolean isZero() {
        return ObjectWritable.empty().equals(value);
    }

    @Override
    public AccumulatorV2<ObjectWritable<A>, ObjectWritable<A>> copy() {
        return new MemoryAccumulator<>(this.memoryComputeKey, this.value);
    }

    @Override
    public void reset() {
        this.value = ObjectWritable.empty();
    }

    @Override
    public void add(final ObjectWritable<A> v) {
        if (this.value.isEmpty())
            this.value = v;
        else if (!v.isEmpty())
            this.value = new ObjectWritable<>(this.memoryComputeKey.getReducer().apply(value.get(), v.get()));
    }

    @Override
    public void merge(final AccumulatorV2<ObjectWritable<A>, ObjectWritable<A>> other) {
        this.add(other.value());
    }

    @Override
    public ObjectWritable<A> value() {
        return this.value;
    }
}
