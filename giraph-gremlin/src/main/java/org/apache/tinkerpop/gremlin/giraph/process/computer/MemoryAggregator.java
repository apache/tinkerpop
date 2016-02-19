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
package org.apache.tinkerpop.gremlin.giraph.process.computer;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MemoryAggregator implements Aggregator<ObjectWritable> {

    private ObjectWritable currentObject;
    private MemoryComputeKey memoryComputeKey;

    public MemoryAggregator() { // for Giraph serialization

    }

    public MemoryAggregator(final MemoryComputeKey memoryComputeKey) {
        this.currentObject = ObjectWritable.empty();
        this.memoryComputeKey = memoryComputeKey;
    }

    @Override
    public ObjectWritable getAggregatedValue() {
        return this.currentObject;
    }

    @Override
    public void setAggregatedValue(final ObjectWritable object) {
        if (null == object)
            this.currentObject = ObjectWritable.empty();
        else if (object.get() instanceof MemoryComputeKey)
            this.memoryComputeKey = (MemoryComputeKey) object.get();
        else
            this.currentObject = object;
    }

    @Override
    public void reset() {
        this.currentObject = ObjectWritable.empty();
    }

    @Override
    public ObjectWritable createInitialValue() {
        return ObjectWritable.empty();
    }

    @Override
    public void aggregate(final ObjectWritable object) {
        if (this.currentObject.isEmpty())
            this.currentObject = object;
        else
            this.currentObject.set(this.memoryComputeKey.getReducer().apply(this.currentObject.get(), object.get()));
    }
}