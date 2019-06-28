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
package org.apache.tinkerpop.gremlin.process.traversal.lambda;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

import java.util.function.Predicate;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class PredicateTraversal<S> extends AbstractLambdaTraversal<S, S> {

    private final Predicate predicate;
    private S s;
    private boolean pass;

    public PredicateTraversal(final Object value) {
        this.predicate = value instanceof Predicate ? (Predicate) value : P.eq(value);
    }

    @Override
    public S next() {
        if (this.pass)
            return this.s;
        throw FastNoSuchElementException.instance();
    }

    @Override
    public boolean hasNext() {
        return this.pass;
    }

    @Override
    public void addStart(final Traverser.Admin<S> start) {
        //noinspection unchecked
        this.pass = this.predicate.test(this.s = start.get());
    }

    @Override
    public String toString() {
        return "(" + this.predicate.toString() + ")";
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode() ^ this.predicate.hashCode();
    }
}

