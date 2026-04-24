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

import org.apache.tinkerpop.gremlin.structure.Direction;

/**
 * Represents an edge pattern element in a GQL MATCH clause. An edge connects a source
 * {@link QueryVertex} to a target {@link QueryVertex} with an optional variable name, optional
 * label constraint, and a traversal direction.
 *
 * <p>Direction semantics follow the GQL arrow notation:
 * <ul>
 *   <li>{@code -[:L]->} is {@link Direction#OUT} — edge goes from source to target</li>
 *   <li>{@code <-[:L]-} is {@link Direction#IN} — edge goes from target to source</li>
 *   <li>{@code -[:L]-} is {@link Direction#BOTH} — undirected, either direction matches</li>
 * </ul>
 */
public final class QueryEdge {

    private final String variable;
    private final String label;
    private final Direction direction;
    private final QueryVertex source;
    private final QueryVertex target;

    public QueryEdge(final String variable, final String label, final Direction direction,
                     final QueryVertex source, final QueryVertex target) {
        this.variable = variable;
        this.label = label;
        this.direction = direction;
        this.source = source;
        this.target = target;
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
        if (direction == Direction.OUT) {
            sb.append("]->");
        } else {
            sb.append("]-");
        }
        sb.append(target);
        return sb.toString();
    }
}
