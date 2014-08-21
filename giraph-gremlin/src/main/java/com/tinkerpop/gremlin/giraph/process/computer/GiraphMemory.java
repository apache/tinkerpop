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
public class GiraphMemory extends MasterCompute implements Memory {

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

    public void compute() {
        this.isMasterCompute = true;
        if (!this.isInitialIteration()) { // the master compute is the first evaluation, thus, don't check for termination at start
            if (this.vertexProgram.terminate(this)) {
                this.haltComputation();
            }
        } else {
            this.vertexProgram = VertexProgram.createVertexProgram(ConfUtil.makeApacheConfiguration(this.getConf()));
            this.memoryKeys = new HashSet<String>(this.vertexProgram.getMemoryComputeKeys());
            try {
                for (final String key : this.memoryKeys) {
                    MemoryHelper.validateKey(key);
                    this.registerPersistentAggregator(key, MemoryAggregator.class);
                    this.setAggregatedValue(key, new RuleWritable(RuleWritable.Rule.NO_OP, null)); // for those memory not defined during setup(), necessary to provide a default value
                }
                this.registerPersistentAggregator(Constants.RUNTIME, MemoryAggregator.class);
                this.set(Constants.RUNTIME, System.currentTimeMillis());
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            this.vertexProgram.setup(this);
        }
    }

    public int getIteration() {
        return this.isMasterCompute ? (int) this.getSuperstep() : (int) this.giraphInternalVertex.getSuperstep();
    }

    public long getRuntime() {
        return System.currentTimeMillis() - this.<Long>get(Constants.RUNTIME);
    }

    public Set<String> keys() {
        return this.memoryKeys;
    }

    public <R> R get(final String key) throws IllegalArgumentException {
        //this.checkKey(key);
        final RuleWritable rule = this.isMasterCompute ? this.getAggregatedValue(key) : this.giraphInternalVertex.getAggregatedValue(key);
        if (null == rule)
            throw Memory.Exceptions.memoryDoesNotExist(key);
        else
            return rule.getObject();
    }

    public void set(final String key, Object value) {
        this.checkKeyValue(key, value);
        if (this.isMasterCompute)
            this.setAggregatedValue(key, new RuleWritable(RuleWritable.Rule.SET, value));
        else
            this.giraphInternalVertex.aggregate(key, new RuleWritable(RuleWritable.Rule.SET, value));
    }

    public boolean and(final String key, final boolean bool) {
        this.checkKeyValue(key, bool);
        if (this.isMasterCompute) {  // only called on setup() and terminate()
            Boolean value = this.<RuleWritable>getAggregatedValue(key).<Boolean>getObject();
            value = null == value ? bool : bool && value;
            this.setAggregatedValue(key, new RuleWritable(RuleWritable.Rule.AND, value));
            return value;
        } else {
            this.giraphInternalVertex.aggregate(key, new RuleWritable(RuleWritable.Rule.AND, bool));
            final Boolean result = ((RuleWritable) this.giraphInternalVertex.getAggregatedValue(key)).getObject();
            return null == result ? bool : result;
        }
    }

    public boolean or(final String key, final boolean bool) {
        this.checkKeyValue(key, bool);
        if (this.isMasterCompute) {   // only called on setup() and terminate()
            Boolean value = this.<RuleWritable>getAggregatedValue(key).<Boolean>getObject();
            value = null == value ? bool : bool || value;
            this.setAggregatedValue(key, new RuleWritable(RuleWritable.Rule.OR, value));
            return value;
        } else {
            this.giraphInternalVertex.aggregate(key, new RuleWritable(RuleWritable.Rule.OR, bool));
            final Boolean result = ((RuleWritable) this.giraphInternalVertex.getAggregatedValue(key)).getObject();
            return null == result ? bool : result;
        }
    }

    public long incr(final String key, final long delta) {
        this.checkKeyValue(key, delta);
        if (this.isMasterCompute) {   // only called on setup() and terminate()
            Number value = this.<RuleWritable>getAggregatedValue(key).<Number>getObject();
            value = null == value ? delta : value.longValue() + delta;
            this.setAggregatedValue(key, new RuleWritable(RuleWritable.Rule.INCR, value));
            return value.longValue();
        } else {
            this.giraphInternalVertex.aggregate(key, new RuleWritable(RuleWritable.Rule.INCR, delta));
            final Long result = ((RuleWritable) this.giraphInternalVertex.getAggregatedValue(key)).getObject();
            return null == result ? delta : result;
        }
    }

    public void write(final DataOutput output) {
        // no need to serialize the master compute as it gets its data from aggregators
        // is this true?
    }

    public void readFields(final DataInput input) {
        // no need to serialize the master compute as it gets its data from aggregators
        // is this true?
    }

    public String toString() {
        return StringFactory.computeMemoryString(this);
    }

    private void checkKeyValue(final String key, final Object value) {
        if (!key.equals(Constants.RUNTIME) && !this.memoryKeys.contains(key))
            throw GraphComputer.Exceptions.providedKeyIsNotAMemoryKey(key);
        MemoryHelper.validateValue(value);
    }
}
