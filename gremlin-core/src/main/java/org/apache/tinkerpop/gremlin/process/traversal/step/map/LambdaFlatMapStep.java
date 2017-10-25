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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class LambdaFlatMapStep<S, E> extends FlatMapStep<S, E> implements LambdaHolder {

    private final Function<Traverser<S>, Iterator<E>> function;

    public LambdaFlatMapStep(final Traversal.Admin traversal, final Function<Traverser<S>, Iterator<E>> function) {
        super(traversal);
        this.function = function;
    }

    public Function<Traverser<S>, Iterator<E>> getFunction() {
        return function;
    }

    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<S> traverser) {
        return this.function.apply(traverser);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.function);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.function.hashCode();
    }
}
