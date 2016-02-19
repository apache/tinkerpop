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

import org.apache.commons.configuration.Configuration;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.MapMemory;
import org.apache.tinkerpop.gremlin.process.computer.util.MemoryHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.javatuples.Pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GiraphMemory extends MasterCompute implements Memory {

    private VertexProgram<?> vertexProgram;
    private GiraphWorkerContext worker;
    private Map<String, MemoryComputeKey> memoryKeys;
    private boolean isMasterCompute = true;
    private long startTime = System.currentTimeMillis();

    public GiraphMemory() {
        // Giraph ReflectionUtils requires this to be public at minimum
    }

    public GiraphMemory(final GiraphWorkerContext worker, final VertexProgram<?> vertexProgram) {
        this.worker = worker;
        this.vertexProgram = vertexProgram;
        this.memoryKeys = new HashMap<>();
        this.vertexProgram.getMemoryComputeKeys().forEach(key -> this.memoryKeys.put(key.getKey(), key));
        this.isMasterCompute = false;
    }


    @Override
    public void initialize() {
        // do not initialize aggregators here because the getConf() configuration is not available at this point
        // use compute() initial iteration instead
    }

    @Override
    public void compute() {
        this.isMasterCompute = true;
        if (0 == this.getSuperstep()) { // setup
            final Configuration apacheConfiguration = ConfUtil.makeApacheConfiguration(this.getConf());
            this.vertexProgram = VertexProgram.createVertexProgram(HadoopGraph.open(apacheConfiguration), apacheConfiguration);
            this.memoryKeys = new HashMap<>();
            this.vertexProgram.getMemoryComputeKeys().forEach(key -> this.memoryKeys.put(key.getKey(), key));
            try {
                for (final MemoryComputeKey key : this.memoryKeys.values()) {
                    this.registerPersistentAggregator(key.getKey(), MemoryAggregator.class);
                }
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            this.vertexProgram.setup(this);
        } else {
            if (this.vertexProgram.terminate(this)) { // terminate
                // write the memory to HDFS
                final MapMemory memory = new MapMemory(this);
                // a hack to get the last iteration memory values to stick
                this.vertexProgram.terminate(memory);
                final String outputLocation = this.getConf().get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, null);
                if (null != outputLocation) {
                    try {
                        for (final String key : this.keys()) {
                            final SequenceFile.Writer writer = SequenceFile.createWriter(FileSystem.get(this.getConf()), this.getConf(), new Path(outputLocation + "/" + key), ObjectWritable.class, ObjectWritable.class);
                            writer.append(ObjectWritable.getNullObjectWritable(), new ObjectWritable<>(memory.get(key)));
                            writer.close();
                        }
                        final SequenceFile.Writer writer = SequenceFile.createWriter(FileSystem.get(this.getConf()), this.getConf(), new Path(outputLocation + "/" + Constants.HIDDEN_ITERATION), ObjectWritable.class, ObjectWritable.class);
                        writer.append(ObjectWritable.getNullObjectWritable(), new ObjectWritable<>(memory.getIteration()));
                        writer.close();
                    } catch (final Exception e) {
                        throw new IllegalStateException(e.getMessage(), e);
                    }
                }
                this.haltComputation();
            }
        }
    }

    @Override
    public int getIteration() {
        if (this.isMasterCompute) {
            final int temp = (int) this.getSuperstep();
            return temp == 0 ? temp : temp - 1;
        } else {
            return (int) this.worker.getSuperstep();
        }
    }

    @Override
    public long getRuntime() {
        return System.currentTimeMillis() - this.startTime;
    }

    @Override
    public Set<String> keys() {
        return this.memoryKeys.values().stream().filter(key -> this.exists(key.getKey())).map(MemoryComputeKey::getKey).collect(Collectors.toSet());
    }

    @Override
    public boolean exists(final String key) {
        final Object value = this.isMasterCompute ? this.getAggregatedValue(key) : this.worker.getAggregatedValue(key);
        return null != value;
    }

    @Override
    public <R> R get(final String key) throws IllegalArgumentException {
        //this.checkKey(key);
        final ObjectWritable<Pair<BinaryOperator, Object>> value = this.isMasterCompute ?
                this.<ObjectWritable<Pair<BinaryOperator, Object>>>getAggregatedValue(key) :
                this.worker.<ObjectWritable<Pair<BinaryOperator, Object>>>getAggregatedValue(key);
        if (null == value || value.isEmpty())
            throw Memory.Exceptions.memoryDoesNotExist(key);
        else
            return (R) value.get().getValue1();
    }

    @Override
    public void set(final String key, final Object value) {
        this.checkKeyValue(key, value);
        if (this.isMasterCompute) {  // only called on setup() and terminate()
            this.setAggregatedValue(key, new ObjectWritable<>(new Pair<>(this.memoryKeys.get(key).getReducer(), value)));
        } else {
            this.worker.aggregate(key, new ObjectWritable<>(new Pair<>(this.memoryKeys.get(key).getReducer(), value)));
        }
    }

    @Override
    public void add(final String key, final Object value) {
        this.checkKeyValue(key, value);
        if (this.isMasterCompute) {  // only called on setup() and terminate()
            this.setAggregatedValue(key, new ObjectWritable<>(new Pair<>(this.memoryKeys.get(key).getReducer(), value)));
        } else {
            this.worker.aggregate(key, new ObjectWritable<>(new Pair<>(this.memoryKeys.get(key).getReducer(), value)));
        }
    }

    @Override
    public void write(final DataOutput output) {
        // all aggregator data is propagated through writables
    }

    @Override
    public void readFields(final DataInput input) {
        // all aggregator data is propagated through writables
    }

    @Override
    public String toString() {
        return StringFactory.memoryString(this);
    }

    private void checkKeyValue(final String key, final Object value) {
        if (!this.memoryKeys.containsKey(key))
            throw GraphComputer.Exceptions.providedKeyIsNotAMemoryComputeKey(key);
        MemoryHelper.validateValue(value);
    }
}
