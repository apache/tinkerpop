package com.tinkerpop.gremlin.giraph.process.olap.util;

import org.apache.giraph.aggregators.BasicAggregator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MemoryAggregator extends BasicAggregator<RuleWritable> {

    public RuleWritable createInitialValue() {
        return new RuleWritable(RuleWritable.Rule.NO_OP, null);
    }

    public void aggregate(RuleWritable t) {
        if (t.getRule().equals(RuleWritable.Rule.AND)) {
            this.setAggregatedValue(new RuleWritable(this.getAggregatedValue().getRule(), this.getAggregatedValue().<Boolean>getObject() && t.<Boolean>getObject()));
        } else if (t.getRule().equals(RuleWritable.Rule.AND)) {
            this.setAggregatedValue(new RuleWritable(this.getAggregatedValue().getRule(), this.getAggregatedValue().<Boolean>getObject() || t.<Boolean>getObject()));
        } else {
            throw new IllegalStateException();
        }
    }
}
