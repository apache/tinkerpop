package com.tinkerpop.gremlin.giraph.process.computer.util;

import org.apache.giraph.aggregators.Aggregator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MemoryAggregator implements Aggregator<RuleWritable> {

    private Object value;
    private RuleWritable.Rule lastRule = null;

    public MemoryAggregator() {
        this.value = null;
    }

    @Override
    public RuleWritable getAggregatedValue() {
        if (null == this.value)
            return createInitialValue();
        else if (this.value instanceof Long)
            return new RuleWritable(RuleWritable.Rule.INCR, this.value);
        else
            return new RuleWritable(null == this.lastRule ? RuleWritable.Rule.NO_OP : this.lastRule, this.value);
    }

    @Override
    public void setAggregatedValue(final RuleWritable rule) {
        this.value = rule.getObject();
    }

    @Override
    public void reset() {
        this.value = null;
    }

    @Override
    public RuleWritable createInitialValue() {
        return new RuleWritable(RuleWritable.Rule.NO_OP, null);
    }

    @Override
    public void aggregate(RuleWritable ruleWritable) {
        final RuleWritable.Rule rule = ruleWritable.getRule();
        final Object object = ruleWritable.getObject();
        if (rule != RuleWritable.Rule.NO_OP)
            this.lastRule = rule;

        if (null == this.value || rule.equals(RuleWritable.Rule.SET)) {
            this.value = object;
        } else {
            if (rule.equals(RuleWritable.Rule.INCR)) {
                this.value = (Long) this.value + (Long) object;
            } else if (rule.equals(RuleWritable.Rule.AND)) {
                this.value = (Boolean) this.value && (Boolean) object;
            } else if (rule.equals(RuleWritable.Rule.OR)) {
                this.value = (Boolean) this.value || (Boolean) object;
            } else if (rule.equals(RuleWritable.Rule.NO_OP)) {
                if (object instanceof Boolean) { // only happens when NO_OP booleans are being propagated will this occur
                    if (null == this.lastRule) {
                        // do nothing ... why?
                    } else if (this.lastRule.equals(RuleWritable.Rule.AND)) {
                        this.value = (Boolean) this.value && (Boolean) object;
                    } else if (this.lastRule.equals(RuleWritable.Rule.OR)) {
                        this.value = (Boolean) this.value || (Boolean) object;
                    } else {
                        throw new IllegalStateException("This state should not have occurred: " + ruleWritable);
                    }
                }
            } else {
                throw new IllegalArgumentException("The provided rule is unknown: " + ruleWritable);
            }
        }
    }
}