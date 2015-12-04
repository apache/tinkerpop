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

import java.io.Serializable;
import java.util.Comparator;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class FunctionComparator<A, B> implements Comparator<A>, Serializable {

    private final Function<A, B> function;
    private final Comparator<B> comparator;

    public FunctionComparator(final Function<A, B> function, final Comparator<B> comparator) {
        this.function = function;
        this.comparator = comparator;
    }

    public Function<A, B> getFunction() {
        return this.function;
    }

    public Comparator<B> getComparator() {
        return this.comparator;
    }

    @Override
    public int compare(final A first, final A second) {
        return this.comparator.compare(this.function.apply(first), this.function.apply(second));
    }

    @Override
    public String toString() {
        return this.comparator.toString() + "(" + this.function + ')';
    }
}
