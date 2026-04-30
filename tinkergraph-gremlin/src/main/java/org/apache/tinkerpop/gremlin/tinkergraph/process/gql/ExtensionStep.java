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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A single join step in a compiled {@link GqlMatchPlan}. An {@code ExtensionStep} describes
 * how to extend a partial match by traversing one edge from an already-bound vertex
 * (the <em>anchor</em>) to a new candidate vertex.
 *
 * <p>The planner emits one {@code ExtensionStep} per {@link QueryEdge} in the
 * {@link QueryGraph}, ordered so that the anchor variable is always bound before the
 * step is executed.
 *
 * <p>Fields:
 * <ul>
 *   <li>{@code anchorVariable} — the variable name of the already-bound vertex from which
 *       traversal begins; never {@code null}</li>
 *   <li>{@code edgeLabel} — optional label constraint on the edge; {@code null} means
 *       any label is accepted</li>
 *   <li>{@code direction} — traversal direction relative to the anchor vertex</li>
 *   <li>{@code edgeVariable} — optional variable name to which the matching edge is bound</li>
 *   <li>{@code targetLabel} — optional label constraint on the target vertex</li>
 *   <li>{@code targetVariable} — optional variable name to which the target vertex is bound</li>
 *   <li>{@code targetPredicates} — property equality predicates on the target vertex,
 *       derived from the inline filter map on the corresponding {@link QueryVertex}</li>
 *   <li>{@code estimatedCost} — static selectivity estimate set by the planner; lower values
 *       indicate a more selective step that should be tried first when multiple steps are
 *       simultaneously eligible. Computed as {@code min(edgeLabelCount, indexedPropertyCount)}.</li>
 * </ul>
 *
 * <p>At runtime, the executor maintains adaptive counters ({@link #recordAttempt()} /
 * {@link #recordHit()}) so that {@link #selectivityRatio()} reflects observed throughput.
 * The executor uses this ratio — with {@code estimatedCost} as a tiebreaker — to choose
 * among simultaneously eligible steps.
 */
public final class ExtensionStep {

    private final String anchorVariable;
    private final String edgeLabel;
    private final Direction direction;
    private final String edgeVariable;
    private final String targetLabel;
    private final String targetVariable;
    private final List<PropertyPredicate> targetPredicates;
    private final long estimatedCost;

    private final AtomicLong attempts = new AtomicLong(0);
    private final AtomicLong hits     = new AtomicLong(0);

    public ExtensionStep(final String anchorVariable, final String edgeLabel,
                         final Direction direction, final String edgeVariable,
                         final String targetLabel, final String targetVariable,
                         final List<PropertyPredicate> targetPredicates,
                         final long estimatedCost) {
        if (anchorVariable == null) throw new IllegalArgumentException("anchorVariable must not be null");
        this.anchorVariable = anchorVariable;
        this.edgeLabel = edgeLabel;
        this.direction = direction;
        this.edgeVariable = edgeVariable;
        this.targetLabel = targetLabel;
        this.targetVariable = targetVariable;
        this.targetPredicates = targetPredicates.isEmpty()
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(targetPredicates));
        this.estimatedCost = estimatedCost;
    }

    public ExtensionStep(final String anchorVariable, final String edgeLabel,
                         final Direction direction, final String edgeVariable,
                         final String targetLabel, final String targetVariable,
                         final List<PropertyPredicate> targetPredicates) {
        this(anchorVariable, edgeLabel, direction, edgeVariable, targetLabel, targetVariable,
             targetPredicates, Long.MAX_VALUE);
    }

    public ExtensionStep(final String anchorVariable, final String edgeLabel,
                         final Direction direction, final String edgeVariable,
                         final String targetLabel, final String targetVariable) {
        this(anchorVariable, edgeLabel, direction, edgeVariable, targetLabel, targetVariable,
             Collections.emptyList());
    }

    /** The variable name of the already-bound vertex used as the starting point for this step. */
    public String getAnchorVariable() {
        return anchorVariable;
    }

    /** The edge label constraint, or {@code null} for any label. */
    public String getEdgeLabel() {
        return edgeLabel;
    }

    /** The traversal direction from the anchor vertex. */
    public Direction getDirection() {
        return direction;
    }

    /** The variable name to which the matching edge is bound, or {@code null}. */
    public String getEdgeVariable() {
        return edgeVariable;
    }

    /** The label constraint on the target vertex, or {@code null} for any label. */
    public String getTargetLabel() {
        return targetLabel;
    }

    /** The variable name to which the matching target vertex is bound, or {@code null}. */
    public String getTargetVariable() {
        return targetVariable;
    }

    /**
     * Property equality predicates that the target vertex must satisfy, derived from
     * the inline filter map on the target {@link QueryVertex}. Empty if no filter was specified.
     */
    public List<PropertyPredicate> getTargetPredicates() {
        return targetPredicates;
    }

    /**
     * Static cost estimate set by the planner. Lower values indicate a more selective step.
     * Computed as {@code min(edgeLabelCount, indexedPropertyCount)} at plan compile time.
     */
    public long getEstimatedCost() {
        return estimatedCost;
    }

    // -------------------------------------------------------------------------
    // Adaptive runtime counters
    // -------------------------------------------------------------------------

    /** Called once per step invocation before iterating candidate edges. */
    void recordAttempt() {
        attempts.incrementAndGet();
    }

    /** Called each time a candidate edge passes all filters and produces a recursive extend call. */
    void recordHit() {
        hits.incrementAndGet();
    }

    /**
     * Observed selectivity ratio: average number of successful extensions per invocation.
     * Laplace-smoothed so new steps start at {@code 1.0} and converge toward the true ratio
     * as observations accumulate. Lower ratios indicate more selective steps.
     */
    double selectivityRatio() {
        return (hits.get() + 1.0) / (attempts.get() + 1.0);
    }

    @Override
    public String toString() {
        return "ExtensionStep{anchor=" + anchorVariable +
               ", edge=" + (edgeVariable != null ? edgeVariable : "_") +
               (edgeLabel != null ? ":" + edgeLabel : "") +
               ", dir=" + direction +
               ", target=" + (targetVariable != null ? targetVariable : "_") +
               (targetLabel != null ? ":" + targetLabel : "") +
               (targetPredicates.isEmpty() ? "" : ", filters=" + targetPredicates) + "}";
    }
}
