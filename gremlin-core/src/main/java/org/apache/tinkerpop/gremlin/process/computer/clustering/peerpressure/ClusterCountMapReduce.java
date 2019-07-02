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
package org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure;

import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration2.Configuration;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ClusterCountMapReduce extends StaticMapReduce<MapReduce.NullObject, Serializable, MapReduce.NullObject, Integer, Integer> {

    public static final String CLUSTER_COUNT_MEMORY_KEY = "gremlin.clusterCountMapReduce.memoryKey";
    public static final String DEFAULT_MEMORY_KEY = "clusterCount";

    private String memoryKey = DEFAULT_MEMORY_KEY;


    private ClusterCountMapReduce() {

    }

    private ClusterCountMapReduce(final String memoryKey) {
        this.memoryKey = memoryKey;
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);
        configuration.setProperty(CLUSTER_COUNT_MEMORY_KEY, this.memoryKey);
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        this.memoryKey = configuration.getString(CLUSTER_COUNT_MEMORY_KEY, DEFAULT_MEMORY_KEY);
    }

    @Override
    public boolean doStage(final Stage stage) {
        return !stage.equals(Stage.COMBINE);
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<NullObject, Serializable> emitter) {
        final Property<Serializable> cluster = vertex.property(PeerPressureVertexProgram.CLUSTER);
        if (cluster.isPresent()) {
            emitter.emit(NullObject.instance(), cluster.value());
        }
    }

    @Override
    public void reduce(final NullObject key, final Iterator<Serializable> values, final ReduceEmitter<NullObject, Integer> emitter) {
        final Set<Serializable> set = new HashSet<>();
        values.forEachRemaining(set::add);
        emitter.emit(NullObject.instance(), set.size());

    }

    @Override
    public Integer generateFinalResult(final Iterator<KeyValue<NullObject, Integer>> keyValues) {
        return keyValues.next().getValue();
    }

    @Override
    public String getMemoryKey() {
        return this.memoryKey;
    }

    @Override
    public String toString() {
        return StringFactory.mapReduceString(this, this.memoryKey);
    }

    //////////////////////////////

    public static Builder build() {
        return new Builder();
    }

    public final static class Builder {

        private String memoryKey = DEFAULT_MEMORY_KEY;

        private Builder() {

        }

        public Builder memoryKey(final String memoryKey) {
            this.memoryKey = memoryKey;
            return this;
        }

        public ClusterCountMapReduce create() {
            return new ClusterCountMapReduce(this.memoryKey);
        }
    }
}