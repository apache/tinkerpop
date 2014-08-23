package com.tinkerpop.gremlin.giraph.process.computer.util;

import org.apache.giraph.aggregators.BasicAggregator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MemoryAggregator extends BasicAggregator<RuleWritable> {

    public RuleWritable createInitialValue() {
        return new RuleWritable(RuleWritable.Rule.SET, null);
    }

    public void aggregate(RuleWritable ruleWritable) {
        final RuleWritable.Rule rule = ruleWritable.getRule();
        final Object object = ruleWritable.getObject();
        if (null == this.getAggregatedValue().getObject() || rule.equals(RuleWritable.Rule.SET))
            this.setAggregatedValue(ruleWritable);
        else {
            if (rule.equals(RuleWritable.Rule.AND)) {
                this.setAggregatedValue(new RuleWritable(rule, this.getAggregatedValue().<Boolean>getObject() && (Boolean) object));
            } else if (rule.equals(RuleWritable.Rule.OR)) {
                this.setAggregatedValue(new RuleWritable(rule, this.getAggregatedValue().<Boolean>getObject() || (Boolean) object));
            } else if (rule.equals(RuleWritable.Rule.INCR)) {
                this.setAggregatedValue(new RuleWritable(rule, this.getAggregatedValue().<Long>getObject() + (Long) object));
            } else {
                throw new IllegalArgumentException("The provided rule is unknown: " + ruleWritable.getRule());
            }
        }
    }
}