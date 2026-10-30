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
package org.apache.tinkerpop.gremlin.gql;

import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents an edge pattern element in a GQL MATCH clause. An edge connects a source
 * {@link QueryVertex} to a target {@link QueryVertex} with an optional variable name, optional
 * label constraint, a traversal direction, and optional property predicates on the edge itself.
 *
 * <p>Direction semantics follow the GQL arrow notation:
 * <ul>
 *   <li>{@code -[:L]->} is {@link Direction#OUT} — edge goes from source to target</li>
 *   <li>{@code <-[:L]-} is {@link Direction#IN} — edge goes from target to source</li>
 *   <li>{@code -[:L]-} is {@link Direction#BOTH} — undirected, either direction matches</li>
 * </ul>
 */
public final class QueryEdge {

    /**
     * Sentinel {@link #getMaxHops()} value denoting an open-ended upper bound, used for the
     * {@code {m,}} and {@code +} quantifier forms (i.e. "at least {@code minHops}, no ceiling").
     */
    public static final int UNBOUNDED = Integer.MAX_VALUE;

    private final String variable;
    private final String label;
    private final Direction direction;
    private final QueryVertex source;
    private final QueryVertex target;
    private final List<PropertyPredicate> predicates;
    private final int minHops;
    private final int maxHops;
    private final boolean quantified;

    /**
     * Creates an edge pattern element. {@code quantified} records whether the pattern carried an
     * explicit quantifier, so {@code {1}} and {@code {1,1}} are quantified even though their
     * bounds equal those of a plain edge.
     */
    public QueryEdge(final String variable, final String label, final Direction direction,
                     final QueryVertex source, final QueryVertex target,
                     final List<PropertyPredicate> predicates,
                     final int minHops, final int maxHops, final boolean quantified) {
        this.variable = variable;
        this.label = label;
        this.direction = direction;
        this.source = source;
        this.target = target;
        this.predicates = predicates.isEmpty()
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(predicates));
        this.minHops = minHops;
        this.maxHops = maxHops;
        this.quantified = quantified;
    }

    public QueryEdge(final String variable, final String label, final Direction direction,
                     final QueryVertex source, final QueryVertex target,
                     final List<PropertyPredicate> predicates) {
        this(variable, label, direction, source, target, predicates, 1, 1, false);
    }

    public QueryEdge(final String variable, final String label, final Direction direction,
                     final QueryVertex source, final QueryVertex target) {
        this(variable, label, direction, source, target, Collections.emptyList(), 1, 1, false);
    }

    /**
     * Returns the variable name bound to this edge, or {@code null} if anonymous.
     */
    public String getVariable() {
        return variable;
    }

    /**
     * Returns the label constraint for this edge, or {@code null} if unconstrained.
     */
    public String getLabel() {
        return label;
    }

    /**
     * Returns the traversal direction: {@link Direction#OUT}, {@link Direction#IN},
     * or {@link Direction#BOTH}.
     */
    public Direction getDirection() {
        return direction;
    }

    /**
     * Returns the source (left-hand) vertex of this edge pattern.
     */
    public QueryVertex getSource() {
        return source;
    }

    /**
     * Returns the target (right-hand) vertex of this edge pattern.
     */
    public QueryVertex getTarget() {
        return target;
    }

    /**
     * Returns the property equality predicates on this edge, or an empty list if none were specified.
     */
    public List<PropertyPredicate> getPredicates() {
        return predicates;
    }

    /**
     * Returns the minimum number of hops this edge pattern matches. For a plain, non-quantified
     * edge this is {@code 1}.
     */
    public int getMinHops() {
        return minHops;
    }

    /**
     * Returns the maximum number of hops this edge pattern matches, or {@link #UNBOUNDED} for the
     * open-ended forms ({@code {m,}} and {@code +}). For a plain, non-quantified edge this is {@code 1}.
     */
    public int getMaxHops() {
        return maxHops;
    }

    /**
     * Returns {@code true} if this edge pattern carries a quantifier (e.g. {@code {1,3}}, {@code {1}},
     * {@code +}, {@code {2,}}), as opposed to a plain edge. This reflects the presence of the
     * quantifier in the query, not the hop bounds, so {@code {1}} and {@code {1,1}} are quantified.
     */
    public boolean isQuantified() {
        return quantified;
    }

    /**
     * Returns {@code true} if this edge's variable is a group variable, which binds an ordered
     * {@code List<Edge>}. That is the case exactly when the edge is quantified and named.
     */
    public boolean isEdgeVariableGroup() {
        return quantified && variable != null;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(source);
        if (direction == Direction.IN) {
            sb.append("<-[");
        } else {
            sb.append("-[");
        }
        if (variable != null) sb.append(variable);
        if (label != null) sb.append(':').append(label);
        if (!predicates.isEmpty()) sb.append(' ').append(predicates);
        if (direction == Direction.OUT) {
            sb.append("]->");
        } else {
            sb.append("]-");
        }
        if (isQuantified()) {
            sb.append('{').append(minHops).append(',');
            if (maxHops != UNBOUNDED) sb.append(maxHops);
            sb.append('}');
        }
        sb.append(target);
        return sb.toString();
    }
}
