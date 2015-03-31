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
package org.apache.tinkerpop.gremlin.hadoop.process.computer.giraph;

import org.apache.giraph.master.MasterCompute;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.MemoryHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphMemory extends MasterCompute implements Memory {

    private VertexProgram<?> vertexProgram;
    private GiraphWorkerContext worker;
    private Set<String> memoryKeys;
    private boolean isMasterCompute = true;

    public GiraphMemory() {
        // Giraph ReflectionUtils requires this to be public at minimum
    }

    public GiraphMemory(final GiraphWorkerContext worker, final VertexProgram<?> vertexProgram) {
        this.worker = worker;
        this.vertexProgram = vertexProgram;
        this.memoryKeys = new HashSet<>(this.vertexProgram.getMemoryComputeKeys());
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
            this.vertexProgram = VertexProgram.createVertexProgram(ConfUtil.makeApacheConfiguration(this.getConf()));
            this.memoryKeys = new HashSet<>(this.vertexProgram.getMemoryComputeKeys());
            try {
                for (final String key : this.memoryKeys) {
                    MemoryHelper.validateKey(key);
                    this.registerPersistentAggregator(key, MemoryAggregator.class);
                }
                this.registerPersistentAggregator(Constants.GREMLIN_HADOOP_HALT, MemoryAggregator.class);
                this.registerPersistentAggregator(Constants.HIDDEN_RUNTIME, MemoryAggregator.class);
                this.setAggregatedValue(Constants.GREMLIN_HADOOP_HALT, new RuleWritable(RuleWritable.Rule.SET, false));
                this.set(Constants.HIDDEN_RUNTIME, System.currentTimeMillis());
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            this.vertexProgram.setup(this);
        } else {
            if (this.get(Constants.GREMLIN_HADOOP_HALT)) {
                this.haltComputation();
            } else if (this.vertexProgram.terminate(this)) { // terminate
                if (!this.getConf().getBoolean(Constants.GREMLIN_HADOOP_DERIVE_MEMORY, false)) // no need for the extra BSP round if memory is not required
                    this.haltComputation();
                else
                    this.setAggregatedValue(Constants.GREMLIN_HADOOP_HALT, new RuleWritable(RuleWritable.Rule.SET, true));
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
        return System.currentTimeMillis() - this.<Long>get(Constants.HIDDEN_RUNTIME);
    }

    @Override
    public Set<String> keys() {
        return this.memoryKeys;
    }

    @Override
    public boolean exists(final String key) {
        final RuleWritable rule = this.isMasterCompute ? this.getAggregatedValue(key) : this.worker.getAggregatedValue(key);
        return null != rule.getObject();
    }

    @Override
    public <R> R get(final String key) throws IllegalArgumentException {
        //this.checkKey(key);
        final RuleWritable rule = this.isMasterCompute ? this.getAggregatedValue(key) : this.worker.getAggregatedValue(key);
        if (null == rule.getObject())
            throw Memory.Exceptions.memoryDoesNotExist(key);
        else
            return rule.getObject();
    }

    @Override
    public void set(final String key, Object value) {
        this.checkKeyValue(key, value);
        if (this.isMasterCompute)
            this.setAggregatedValue(key, new RuleWritable(RuleWritable.Rule.SET, value));
        else
            this.worker.aggregate(key, new RuleWritable(RuleWritable.Rule.SET, value));
    }

    @Override
    public void and(final String key, final boolean bool) {
        this.checkKeyValue(key, bool);
        if (this.isMasterCompute) {  // only called on setup() and terminate()
            Boolean value = this.<RuleWritable>getAggregatedValue(key).<Boolean>getObject();
            value = null == value ? bool : bool && value;
            this.setAggregatedValue(key, new RuleWritable(RuleWritable.Rule.AND, value));
        } else {
            this.worker.aggregate(key, new RuleWritable(RuleWritable.Rule.AND, bool));
        }
    }

    @Override
    public void or(final String key, final boolean bool) {
        this.checkKeyValue(key, bool);
        if (this.isMasterCompute) {   // only called on setup() and terminate()
            Boolean value = this.<RuleWritable>getAggregatedValue(key).<Boolean>getObject();
            value = null == value ? bool : bool || value;
            this.setAggregatedValue(key, new RuleWritable(RuleWritable.Rule.OR, value));
        } else {
            this.worker.aggregate(key, new RuleWritable(RuleWritable.Rule.OR, bool));
        }
    }

    @Override
    public void incr(final String key, final long delta) {
        this.checkKeyValue(key, delta);
        if (this.isMasterCompute) {   // only called on setup() and terminate()
            Number value = this.<RuleWritable>getAggregatedValue(key).<Number>getObject();
            value = null == value ? delta : value.longValue() + delta;
            this.setAggregatedValue(key, new RuleWritable(RuleWritable.Rule.INCR, value));
        } else {
            this.worker.aggregate(key, new RuleWritable(RuleWritable.Rule.INCR, delta));
        }
    }

    @Override
    public void write(final DataOutput output) {
        // no need to serialize the master compute as it gets its data from aggregators
        // is this true?
    }

    @Override
    public void readFields(final DataInput input) {
        // no need to serialize the master compute as it gets its data from aggregators
        // is this true?
    }

    @Override
    public String toString() {
        return StringFactory.memoryString(this);
    }

    private void checkKeyValue(final String key, final Object value) {
        if (!key.equals(Constants.HIDDEN_RUNTIME) && !this.memoryKeys.contains(key))
            throw GraphComputer.Exceptions.providedKeyIsNotAMemoryComputeKey(key);
        MemoryHelper.validateValue(value);
    }
}
