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

import org.apache.tinkerpop.gremlin.structure.Element;

import java.io.Serializable;
import java.util.Comparator;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ElementFunctionComparator<V> implements Comparator<Element>, Serializable {

    private final Function<Element, V> elementFunction;
    private final Comparator<V> valueComparator;

    public ElementFunctionComparator(final Function<Element, V> elementFunction, final Comparator<V> valueComparator) {
        this.elementFunction = elementFunction;
        this.valueComparator = valueComparator;
    }

    public Function<Element, V> getElementFunction() {
        return this.elementFunction;
    }

    public Comparator<V> getValueComparator() {
        return this.valueComparator;
    }

    @Override
    public int compare(final Element elementA, final Element elementB) {
        return this.valueComparator.compare(this.elementFunction.apply(elementA), this.elementFunction.apply(elementB));
    }

    @Override
    public String toString() {
        return this.valueComparator.toString() + "(" + this.elementFunction + ')';
    }
}
