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

import org.apache.giraph.aggregators.Aggregator;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.util.Rule;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MemoryAggregator implements Aggregator<Rule> {

    private Object currentObject;
    private Rule.Operation lastOperation = null;

    public MemoryAggregator() {
        this.currentObject = null;
    }

    @Override
    public Rule getAggregatedValue() {
        if (null == this.currentObject)
            return createInitialValue();
        else if (this.currentObject instanceof Long)
            return new Rule(Rule.Operation.INCR, this.currentObject);
        else
            return new Rule(null == this.lastOperation ? Rule.Operation.NO_OP : this.lastOperation, this.currentObject);
    }

    @Override
    public void setAggregatedValue(final Rule rule) {
        this.currentObject = null == rule ? null : rule.getObject();
    }

    @Override
    public void reset() {
        this.currentObject = null;
    }

    @Override
    public Rule createInitialValue() {
        return new Rule(Rule.Operation.NO_OP, null);
    }

    @Override
    public void aggregate(final Rule ruleWritable) {
        final Rule.Operation rule = ruleWritable.getOperation();
        final Object object = ruleWritable.getObject();
        if (rule != Rule.Operation.NO_OP)
            this.lastOperation = rule;

        if (null == this.currentObject || rule.equals(Rule.Operation.SET)) {
            this.currentObject = object;
        } else {
            if (rule.equals(Rule.Operation.INCR)) {
                this.currentObject = (Long) this.currentObject + (Long) object;
            } else if (rule.equals(Rule.Operation.AND)) {
                this.currentObject = (Boolean) this.currentObject && (Boolean) object;
            } else if (rule.equals(Rule.Operation.OR)) {
                this.currentObject = (Boolean) this.currentObject || (Boolean) object;
            } else if (rule.equals(Rule.Operation.NO_OP)) {
                if (object instanceof Boolean) { // only happens when NO_OP booleans are being propagated will this occur
                    if (null == this.lastOperation) {
                        // do nothing ... why?
                    } else if (this.lastOperation.equals(Rule.Operation.AND)) {
                        this.currentObject = (Boolean) this.currentObject && (Boolean) object;
                    } else if (this.lastOperation.equals(Rule.Operation.OR)) {
                        this.currentObject = (Boolean) this.currentObject || (Boolean) object;
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