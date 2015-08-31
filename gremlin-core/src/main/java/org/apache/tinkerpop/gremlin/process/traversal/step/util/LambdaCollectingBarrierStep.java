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

package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;

import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class LambdaCollectingBarrierStep<S> extends CollectingBarrierStep<S> implements LambdaHolder {

    public enum Consumers implements Consumer<TraverserSet<Object>> {
        noOp {
            @Override
            public void accept(final TraverserSet<Object> traverserSet) {

            }
        }, normSack {
            @Override
            public void accept(final TraverserSet<Object> traverserSet) {
                double total = 0.0d;
                for (final Traverser.Admin<Object> traverser : traverserSet) {
                    total = total + ((double) traverser.sack() * (double) traverser.bulk());
                }
                for (final Traverser.Admin<Object> traverser : traverserSet) {
                    traverser.sack(((double) traverser.sack() * (double) traverser.bulk()) / total);
                }
            }
        }
    }


    private final Consumer<TraverserSet<S>> barrierConsumer;

    public LambdaCollectingBarrierStep(final Traversal.Admin traversal, final Consumer<TraverserSet<S>> barrierConsumer, final int maxBarrierSize) {
        super(traversal, maxBarrierSize);
        this.barrierConsumer = barrierConsumer;
    }

    @Override
    public void barrierConsumer(final TraverserSet<S> traverserSet) {
        this.barrierConsumer.accept(traverserSet);
    }
}
