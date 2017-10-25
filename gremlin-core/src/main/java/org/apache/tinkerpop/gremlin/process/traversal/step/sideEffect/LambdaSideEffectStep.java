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
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class LambdaSideEffectStep<S> extends SideEffectStep<S> implements LambdaHolder {

    private final Consumer<Traverser<S>> consumer;

    public LambdaSideEffectStep(final Traversal.Admin traversal, final Consumer<Traverser<S>> consumer) {
        super(traversal);
        this.consumer = consumer;
    }

    public Consumer<Traverser<S>> getConsumer() {
        return consumer;
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        this.consumer.accept(traverser);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.consumer);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.consumer.hashCode();
    }
}
