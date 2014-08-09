package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.giraph.Constants;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.process.computer.util.MemoryAggregator;
import com.tinkerpop.gremlin.giraph.process.computer.util.RuleWritable;
import com.tinkerpop.gremlin.giraph.structure.util.GiraphInternalVertex;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.SideEffects;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.giraph.master.MasterCompute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphComputerSideEffects extends MasterCompute implements SideEffects {

    private final Logger LOGGER = LoggerFactory.getLogger(GiraphGraphComputerSideEffects.class);
    private VertexProgram vertexProgram;
    private GiraphInternalVertex giraphInternalVertex;
    private Set<String> sideEffectKeys;

    public GiraphGraphComputerSideEffects() {
        this.giraphInternalVertex = null;
        this.vertexProgram = null;
        this.initialize();
    }

    public GiraphGraphComputerSideEffects(final GiraphInternalVertex giraphInternalVertex) {
        this.giraphInternalVertex = giraphInternalVertex;
        this.initialize();
    }

    public void initialize() {
        if (null == this.giraphInternalVertex) {  // master compute node
            try {
                this.vertexProgram = VertexProgram.createVertexProgram(ConfUtil.makeApacheConfiguration(this.getConf()));
                this.sideEffectKeys = new HashSet<String>(this.vertexProgram.getSideEffectComputeKeys());
                for (final String key : (Set<String>) this.vertexProgram.getSideEffectComputeKeys()) {
                    this.registerAggregator(key, MemoryAggregator.class); // TODO: Why does PersistentAggregator not work?
                }
                this.registerPersistentAggregator(Constants.RUNTIME, MemoryAggregator.class);
                this.setIfAbsent(Constants.RUNTIME, System.currentTimeMillis());
                this.vertexProgram.setup(this);
            } catch (final Exception e) {
                LOGGER.error(e.getMessage(), e);
                // do nothing as Giraph has a hard time starting up with random exceptions until ZooKeeper comes online
            }
        } else {  // local vertex aggregator
            this.vertexProgram = VertexProgram.createVertexProgram(ConfUtil.makeApacheConfiguration(this.giraphInternalVertex.getConf()));
            this.sideEffectKeys = new HashSet<String>(this.vertexProgram.getSideEffectComputeKeys());
        }
    }

    // TODO: Perhaps wait for setConfiguration? ^^^^^^^

    public void compute() {
        if (!this.isInitialIteration()) {
            if (this.vertexProgram.terminate(this)) {
                this.haltComputation();
            }
        }
    }

    public int getIteration() {
        return null == this.giraphInternalVertex ? (int) this.getSuperstep() : (int) this.giraphInternalVertex.getSuperstep();
    }

    public long getRuntime() {
        return System.currentTimeMillis() - this.<Long>get(Constants.RUNTIME).get();
    }

    public Set<String> keys() {
        return this.sideEffectKeys;
    }

    public <R> Optional<R> get(final String key) {
        this.checkKey(key);
        final RuleWritable rule = (null == this.giraphInternalVertex) ? this.getAggregatedValue(key) : this.giraphInternalVertex.getAggregatedValue(key);
        return Optional.ofNullable(rule.getObject());
    }

    public void set(final String key, Object value) {
        this.checkKey(key);
        if (null == this.giraphInternalVertex)
            this.setAggregatedValue(key, new RuleWritable(RuleWritable.Rule.SET, value));
        else
            this.giraphInternalVertex.aggregate(key, new RuleWritable(RuleWritable.Rule.SET, value));
    }

    public void setIfAbsent(final String key, final Object value) {
        this.checkKey(key);
        if (null == this.giraphInternalVertex)
            this.setAggregatedValue(key, new RuleWritable(RuleWritable.Rule.SET_IF_ABSENT, value));
        else
            this.giraphInternalVertex.aggregate(key, new RuleWritable(RuleWritable.Rule.SET_IF_ABSENT, value));
    }

    public boolean and(final String key, final boolean bool) {
        this.checkKey(key);
        if (null == this.giraphInternalVertex) {
            this.setAggregatedValue(key, new RuleWritable(RuleWritable.Rule.AND, ((RuleWritable) this.getAggregatedValue(key)).<Boolean>getObject() && bool));
            return ((RuleWritable) this.getAggregatedValue(key)).getObject();
        } else {
            this.giraphInternalVertex.aggregate(key, new RuleWritable(RuleWritable.Rule.AND, bool));
            final Boolean result = ((RuleWritable) this.giraphInternalVertex.getAggregatedValue(key)).getObject();
            return null == result ? bool : result;
        }
    }

    public boolean or(final String key, final boolean bool) {
        this.checkKey(key);
        if (null == this.giraphInternalVertex) {
            this.setAggregatedValue(key, new RuleWritable(RuleWritable.Rule.OR, ((RuleWritable) this.getAggregatedValue(key)).<Boolean>getObject() || bool));
            return ((RuleWritable) this.getAggregatedValue(key)).getObject();
        } else {
            this.giraphInternalVertex.aggregate(key, new RuleWritable(RuleWritable.Rule.OR, bool));
            final Boolean result = ((RuleWritable) this.giraphInternalVertex.getAggregatedValue(key)).getObject();
            return null == result ? bool : result;
        }
    }

    public long incr(final String key, final long delta) {
        this.checkKey(key);
        if (null == this.giraphInternalVertex) {
            this.setAggregatedValue(key, new RuleWritable(RuleWritable.Rule.INCR, ((RuleWritable) this.getAggregatedValue(key)).<Long>getObject() + delta));
            return ((RuleWritable) this.getAggregatedValue(key)).getObject();
        } else {
            this.giraphInternalVertex.aggregate(key, new RuleWritable(RuleWritable.Rule.INCR, delta));
            final Long result = ((RuleWritable) this.giraphInternalVertex.getAggregatedValue(key)).getObject();
            return null == result ? delta : result;
        }
    }

    public void write(final DataOutput output) {
    }

    public void readFields(final DataInput input) {
    }

    public String toString() {
        return StringFactory.computerSideEffectsString(this);
    }

    private void checkKey(final String key) {
        if (!key.equals(Constants.RUNTIME) && !this.sideEffectKeys.contains(key))
            throw GraphComputer.Exceptions.providedKeyIsNotASideEffectKey(key);
    }
}
