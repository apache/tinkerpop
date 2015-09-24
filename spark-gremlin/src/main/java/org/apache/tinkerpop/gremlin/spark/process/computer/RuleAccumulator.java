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
package org.apache.tinkerpop.gremlin.spark.process.computer;

import org.apache.spark.AccumulatorParam;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.util.Rule;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RuleAccumulator implements AccumulatorParam<Rule> {

    @Override
    public Rule addAccumulator(final Rule a, final Rule b) {
        if (a.getOperation().equals(Rule.Operation.NO_OP))
            return b;
        if (b.getOperation().equals(Rule.Operation.NO_OP))
            return a;
        else
            return new Rule(b.getOperation(), b.getOperation().compute(a.getObject(), b.getObject()));
    }

    @Override
    public Rule addInPlace(final Rule a, final Rule b) {
        if (a.getOperation().equals(Rule.Operation.NO_OP))
            return b;
        if (b.getOperation().equals(Rule.Operation.NO_OP))
            return a;
        else
            return new Rule(b.getOperation(), b.getOperation().compute(a.getObject(), b.getObject()));
    }

    @Override
    public Rule zero(final Rule rule) {
        return new Rule(Rule.Operation.NO_OP, null);
    }


}
