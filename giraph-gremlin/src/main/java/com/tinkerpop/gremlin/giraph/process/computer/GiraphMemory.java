package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.giraph.Constants;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.process.computer.util.MemoryAggregator;
import com.tinkerpop.gremlin.giraph.process.computer.util.RuleWritable;
import com.tinkerpop.gremlin.giraph.structure.util.GiraphInternalVertex;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.util.MemoryHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.giraph.master.MasterCompute;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GiraphMemory extends MasterCompute implements Memory {

    private VertexProgram vertexProgram;
    private GiraphInternalVertex giraphInternalVertex;
    private Set<String> memoryKeys;
    private boolean isMasterCompute = true;

    public GiraphMemory() {
    }

    public GiraphMemory(final GiraphInternalVertex giraphInternalVertex, final VertexProgram vertexProgram) {
        this.giraphInternalVertex = giraphInternalVertex;
        this.vertexProgram = vertexProgram;
        this.memoryKeys = new HashSet<String>(this.vertexProgram.getMemoryComputeKeys());
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
            this.memoryKeys = new HashSet<String>(this.vertexProgram.getMemoryComputeKeys());
            try {
                for (final String key : this.memoryKeys) {
                    MemoryHelper.validateKey(key);
                    this.registerPersistentAggregator(key, MemoryAggregator.class);
                }
                this.registerPersistentAggregator(Constants.GREMLIN_GIRAPH_HALT, MemoryAggregator.class);
                this.registerPersistentAggregator(Constants.SYSTEM_RUNTIME, MemoryAggregator.class);
                this.setAggregatedValue(Constants.GREMLIN_GIRAPH_HALT, new RuleWritable(RuleWritable.Rule.SET, false));
                this.set(Constants.SYSTEM_RUNTIME, System.currentTimeMillis());
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            this.vertexProgram.setup(this);
        } else {
            if (this.get(Constants.GREMLIN_GIRAPH_HALT)) {
                this.haltComputation();
            } else if (this.vertexProgram.terminate(this)) { // terminate
                if (!this.getConf().getBoolean(Constants.GREMLIN_GIRAPH_DERIVE_MEMORY, false)) // no need for the extra BSP round if memory is not required
                    this.haltComputation();
                else
                    this.setAggregatedValue(Constants.GREMLIN_GIRAPH_HALT, new RuleWritable(RuleWritable.Rule.SET, true));
            }
        }
    }

    @Override
    public int getIteration() {
        if (this.isMasterCompute) {
            final int temp = (int) this.getSuperstep();
            return temp == 0 ? temp : temp - 1;
        } else {
            return (int) this.giraphInternalVertex.getSuperstep();
        }
    }

    @Override
    public long getRuntime() {
        return System.currentTimeMillis() - this.<Long>get(Constants.SYSTEM_RUNTIME);
    }

    @Override
    public Set<String> keys() {
        return this.memoryKeys;
    }

    @Override
    public boolean exists(final String key) {
        final RuleWritable rule = this.isMasterCompute ? this.getAggregatedValue(key) : this.giraphInternalVertex.getAggregatedValue(key);
        return null != rule.getObject();
    }

    @Override
    public <R> R get(final String key) throws IllegalArgumentException {
        //this.checkKey(key);
        final RuleWritable rule = this.isMasterCompute ? this.getAggregatedValue(key) : this.giraphInternalVertex.getAggregatedValue(key);
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
            this.giraphInternalVertex.aggregate(key, new RuleWritable(RuleWritable.Rule.SET, value));
    }

    @Override
    public boolean and(final String key, final boolean bool) {
        this.checkKeyValue(key, bool);
        if (this.isMasterCompute) {  // only called on setup() and terminate()
            Boolean value = this.<RuleWritable>getAggregatedValue(key).<Boolean>getObject();
            value = null == value ? bool : bool && value;
            this.setAggregatedValue(key, new RuleWritable(RuleWritable.Rule.AND, value));
            return value;
        } else {
            final Boolean result = ((RuleWritable) this.giraphInternalVertex.getAggregatedValue(key)).getObject();
            this.giraphInternalVertex.aggregate(key, new RuleWritable(RuleWritable.Rule.AND, bool));
            return null == result ? bool : result && bool;
        }
    }

    @Override
    public boolean or(final String key, final boolean bool) {
        this.checkKeyValue(key, bool);
        if (this.isMasterCompute) {   // only called on setup() and terminate()
            Boolean value = this.<RuleWritable>getAggregatedValue(key).<Boolean>getObject();
            value = null == value ? bool : bool || value;
            this.setAggregatedValue(key, new RuleWritable(RuleWritable.Rule.OR, value));
            return value;
        } else {
            final Boolean result = ((RuleWritable) this.giraphInternalVertex.getAggregatedValue(key)).getObject();
            this.giraphInternalVertex.aggregate(key, new RuleWritable(RuleWritable.Rule.OR, bool));
            return null == result ? bool : result || bool;
        }
    }

    @Override
    public long incr(final String key, final long delta) {
        this.checkKeyValue(key, delta);
        if (this.isMasterCompute) {   // only called on setup() and terminate()
            Number value = this.<RuleWritable>getAggregatedValue(key).<Number>getObject();
            value = null == value ? delta : value.longValue() + delta;
            this.setAggregatedValue(key, new RuleWritable(RuleWritable.Rule.INCR, value));
            return value.longValue();
        } else {
            final Long result = ((RuleWritable) this.giraphInternalVertex.getAggregatedValue(key)).getObject();
            this.giraphInternalVertex.aggregate(key, new RuleWritable(RuleWritable.Rule.INCR, delta));
            return null == result ? delta : result + delta;
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
        if (!key.equals(Constants.SYSTEM_RUNTIME) && !this.memoryKeys.contains(key))
            throw GraphComputer.Exceptions.providedKeyIsNotAMemoryComputeKey(key);
        MemoryHelper.validateValue(value);
    }
}
