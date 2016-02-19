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
import org.javatuples.Pair;

import java.util.function.BinaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MemoryAggregator implements Aggregator<ObjectWritable<Pair<BinaryOperator, Object>>> {

    private ObjectWritable<Pair<BinaryOperator, Object>> currentObject = ObjectWritable.<Pair<BinaryOperator, Object>>empty();

    public MemoryAggregator() { // for Giraph serialization

    }

    @Override
    public ObjectWritable<Pair<BinaryOperator, Object>> getAggregatedValue() {
        return this.currentObject;
    }

    @Override
    public void setAggregatedValue(final ObjectWritable<Pair<BinaryOperator, Object>> object) {
        if (null != object)
            this.currentObject = object;
    }

    @Override
    public void aggregate(final ObjectWritable<Pair<BinaryOperator, Object>> object) {
        if (null == object)
            return;
        else if (this.currentObject.isEmpty())
            this.currentObject = object;
        else if (!object.isEmpty())
            this.currentObject.set(new Pair<>(object.get().getValue0(), object.get().getValue0().apply(this.currentObject.get().getValue1(), object.get().getValue1())));
    }

    @Override
    public void reset() {
        this.currentObject = ObjectWritable.<Pair<BinaryOperator, Object>>empty();
    }

    @Override
    public ObjectWritable<Pair<BinaryOperator, Object>> createInitialValue() {
        return ObjectWritable.<Pair<BinaryOperator, Object>>empty();
    }
}