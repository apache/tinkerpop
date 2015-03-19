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

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ElementValueComparator<V> implements Comparator<Element>, Serializable {

    private final String propertyKey;
    private final Comparator<V> valueComparator;

    public ElementValueComparator(final String propertyKey, final Comparator<V> valueComparator) {
        this.propertyKey = propertyKey;
        this.valueComparator = valueComparator;
    }

    public String getPropertyKey() {
        return this.propertyKey;
    }

    public Comparator<V> getValueComparator() {
        return this.valueComparator;
    }

    @Override
    public int compare(final Element elementA, final Element elementB) {
        return this.valueComparator.compare(elementA.value(this.propertyKey), elementB.value(this.propertyKey));
    }

    @Override
    public String toString() {
        return this.valueComparator.toString() + '(' + this.propertyKey + ')';
    }
}
