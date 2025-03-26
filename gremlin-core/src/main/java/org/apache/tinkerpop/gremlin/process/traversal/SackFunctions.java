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

package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;

import java.util.function.Consumer;

import static org.apache.tinkerpop.gremlin.util.NumberHelper.add;
import static org.apache.tinkerpop.gremlin.util.NumberHelper.div;
import static org.apache.tinkerpop.gremlin.util.NumberHelper.mul;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SackFunctions {

    private SackFunctions() {

    }

    public enum Barrier implements Consumer<TraverserSet<Object>> {
        normSack {
            @Override
            public void accept(final TraverserSet<Object> traverserSet) {
                Number total = 0.0;
                for (final Traverser.Admin<Object> traverser : traverserSet) {
                    total = add(total, mul(GValue.numberOf(traverser.sack()), traverser.bulk()));
                }
                for (final Traverser.Admin<Object> traverser : traverserSet) {
                    traverser.sack(div(mul(GValue.numberOf(traverser.sack()), traverser.bulk()), total));
                }
            }
        }
    }
}
