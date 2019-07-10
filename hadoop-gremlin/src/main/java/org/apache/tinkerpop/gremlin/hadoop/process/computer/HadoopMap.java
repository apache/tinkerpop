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
package org.apache.tinkerpop.gremlin.hadoop.process.computer;

import org.apache.commons.configuration2.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopPools;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShimServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HadoopMap extends Mapper<NullWritable, VertexWritable, ObjectWritable, ObjectWritable> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopMap.class);
    private MapReduce mapReduce;
    private final HadoopMapEmitter<ObjectWritable, ObjectWritable> mapEmitter = new HadoopMapEmitter<>();

    private HadoopMap() {

    }

    @Override
    public void setup(final Mapper<NullWritable, VertexWritable, ObjectWritable, ObjectWritable>.Context context) {
        final Configuration apacheConfiguration = ConfUtil.makeApacheConfiguration(context.getConfiguration());
        KryoShimServiceLoader.applyConfiguration(apacheConfiguration);
        this.mapReduce = MapReduce.createMapReduce(HadoopGraph.open(apacheConfiguration), apacheConfiguration);
        this.mapReduce.workerStart(MapReduce.Stage.MAP);
    }

    @Override
    public void map(final NullWritable key, final VertexWritable value, final Mapper<NullWritable, VertexWritable, ObjectWritable, ObjectWritable>.Context context) throws IOException, InterruptedException {
        this.mapEmitter.setContext(context);
        this.mapReduce.map(ComputerGraph.mapReduce(value.get()), this.mapEmitter);
    }

    @Override
    public void cleanup(final Mapper<NullWritable, VertexWritable, ObjectWritable, ObjectWritable>.Context context) {
        this.mapReduce.workerEnd(MapReduce.Stage.MAP);
    }

    public class HadoopMapEmitter<K, V> implements MapReduce.MapEmitter<K, V> {

        private Mapper<NullWritable, VertexWritable, ObjectWritable, ObjectWritable>.Context context;
        private final ObjectWritable<K> keyWritable = new ObjectWritable<>();
        private final ObjectWritable<V> valueWritable = new ObjectWritable<>();

        public void setContext(final Mapper<NullWritable, VertexWritable, ObjectWritable, ObjectWritable>.Context context) {
            this.context = context;
        }

        @Override
        public void emit(final K key, final V value) {
            this.keyWritable.set(key);
            this.valueWritable.set(value);
            try {
                this.context.write(this.keyWritable, this.valueWritable);
            } catch (final Exception e) {
                LOGGER.error(e.getMessage());
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }
}
