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

package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ColumnTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.FunctionTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.Comparator;
import java.util.function.Function;

/**
 * A {@code ByModulating} step is able to take {@link org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal#by} calls.
 * All the methods have default implementations excecpt {@link ByModulating#modulateBy(Traversal.Admin)}.
 * In short, given a traversal, what should the ByModulating step do with it.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface ByModulating {

    public default void modulateBy(final Traversal.Admin<?, ?> traversal) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("The by()-modulating step does not support traversal-based modulation: " + this);
    }

    public default void modulateBy(final String string) throws UnsupportedOperationException {
        this.modulateBy(new ElementValueTraversal(string));
    }

    public default void modulateBy(final T token) throws UnsupportedOperationException {
        this.modulateBy(new TokenTraversal(token));
    }

    public default void modulateBy(final Function function) throws UnsupportedOperationException {
        if (function instanceof T)
            this.modulateBy((T) function);
        else
            this.modulateBy(__.map(new FunctionTraverser<>(function)).asAdmin());
    }

    public default void modulateBy() throws UnsupportedOperationException {
        this.modulateBy(new IdentityTraversal());
    }

    //////

    public default void modulateBy(final Traversal.Admin<?, ?> traversal, final Comparator comparator) {
        throw new UnsupportedOperationException("The by()-modulating step does not support traversal/comparator-based modulation: " + this);
    }

    public default void modulateBy(final String key, final Comparator comparator) {
        this.modulateBy(new ElementValueTraversal<>(key), comparator);
    }

    public default void modulateBy(final Comparator comparator) {
        this.modulateBy(new IdentityTraversal<>(), comparator);
    }

    public default void modulateBy(final Order order) {
        this.modulateBy(new IdentityTraversal<>(), order);
    }

    public default void modulateBy(final T t, final Comparator comparator) {
        this.modulateBy(new TokenTraversal<>(t), comparator);
    }

    public default void modulateBy(final Column column, final Comparator comparator) {
        this.modulateBy(new ColumnTraversal(column), comparator);
    }

    public default void modulateBy(final Function function, final Comparator comparator) {
        if (function instanceof T)
            this.modulateBy((T) function, comparator);
        else if (function instanceof Column)
            this.modulateBy((Column) function, comparator);
        else
            this.modulateBy(__.map(new FunctionTraverser<>(function)).asAdmin(), comparator);
    }
}
