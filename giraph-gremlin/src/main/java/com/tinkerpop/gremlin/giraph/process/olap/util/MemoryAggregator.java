package com.tinkerpop.gremlin.giraph.process.olap.util;

import org.apache.giraph.aggregators.BasicAggregator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MemoryAggregator extends BasicAggregator<RuleWritable> {

    public RuleWritable createInitialValue() {
        return new RuleWritable(RuleWritable.Rule.NO_OP, null);
    }

    public void aggregate(RuleWritable ruleWritable) {
        if (ruleWritable.getRule().equals(RuleWritable.Rule.AND)) {
            if (null == this.getAggregatedValue().getObject())
                this.setAggregatedValue(new RuleWritable(this.getAggregatedValue().getRule(), ruleWritable.<Boolean>getObject()));
            else
                this.setAggregatedValue(new RuleWritable(this.getAggregatedValue().getRule(), this.getAggregatedValue().<Boolean>getObject() && ruleWritable.<Boolean>getObject()));
        } else if (ruleWritable.getRule().equals(RuleWritable.Rule.OR)) {
            if (null == this.getAggregatedValue().getObject())
                this.setAggregatedValue(new RuleWritable(this.getAggregatedValue().getRule(), ruleWritable.<Boolean>getObject()));
            else
                this.setAggregatedValue(new RuleWritable(this.getAggregatedValue().getRule(), this.getAggregatedValue().<Boolean>getObject() || ruleWritable.<Boolean>getObject()));
        } else if (ruleWritable.getRule().equals(RuleWritable.Rule.NO_OP)) {
            this.setAggregatedValue(ruleWritable);
        } else {
            throw new IllegalArgumentException("The provided rule is unknown: " + ruleWritable.getRule());
        }
    }
}
