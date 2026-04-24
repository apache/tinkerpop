/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.process.gql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents a vertex pattern element in a GQL MATCH clause. A vertex may have an optional
 * variable name for result binding, an optional label constraint for filtering, and an
 * optional list of property predicates from an inline filter map.
 * All fields are nullable/empty: an anonymous vertex {@code ()} has neither variable nor label.
 */
public final class QueryVertex {

    private final String variable;
    private final String label;
    private final List<PropertyPredicate> predicates;

    public QueryVertex(final String variable, final String label,
                     final List<PropertyPredicate> predicates) {
        this.variable = variable;
        this.label = label;
        this.predicates = predicates.isEmpty()
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(predicates));
    }

    public QueryVertex(final String variable, final String label) {
        this(variable, label, Collections.emptyList());
    }

    /**
     * Returns the variable name bound to this vertex, or {@code null} if anonymous.
     */
    public String getVariable() {
        return variable;
    }

    /**
     * Returns the label constraint for this vertex, or {@code null} if unconstrained.
     */
    public String getLabel() {
        return label;
    }

    /**
     * Returns the property predicates parsed from the inline filter map, or an empty list
     * if no property filter was specified.
     */
    public List<PropertyPredicate> getPredicates() {
        return predicates;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("(");
        if (variable != null) sb.append(variable);
        if (label != null) sb.append(':').append(label);
        if (!predicates.isEmpty()) sb.append(' ').append(predicates);
        sb.append(')');
        return sb.toString();
    }
}
