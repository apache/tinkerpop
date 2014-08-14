package com.tinkerpop.gremlin.giraph.process.computer.util;

import org.apache.giraph.aggregators.Aggregator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MemoryAggregator implements Aggregator<RuleWritable> {
    private Object value;


    public MemoryAggregator() {
        this.value = null;
    }

    @Override
    public RuleWritable getAggregatedValue() {
        return new RuleWritable(RuleWritable.Rule.NO_OP, this.value);
    }

    @Override
    public void setAggregatedValue(final RuleWritable value) {
        this.value = value.getObject();
    }

    @Override
    public void reset() {
        this.value = null;
    }

    @Override
    public RuleWritable createInitialValue() {
        return new RuleWritable(RuleWritable.Rule.NO_OP, null);
    }

    public void aggregate(final RuleWritable ruleWritable) {
        final RuleWritable.Rule rule = ruleWritable.getRule();
        final Object object = ruleWritable.getObject();

        if (rule.equals(RuleWritable.Rule.SET)) {
            this.value = object;
        } else if (rule.equals(RuleWritable.Rule.NO_OP)) {
            if (null == this.value)
                this.value = object;
        } else if (rule.equals(RuleWritable.Rule.AND)) {
            this.value = null == this.value ? object : (Boolean) this.value && (Boolean) object;
        } else if (rule.equals(RuleWritable.Rule.OR)) {
            this.value = null == this.value ? object : (Boolean) this.value || (Boolean) object;
        } else if (rule.equals(RuleWritable.Rule.INCR)) {
            this.value = null == this.value ? object : (Long) this.value + (Long) object;
        } else {
            throw new IllegalArgumentException("The provided rule is unknown: " + ruleWritable.getRule());
        }
    }
}

