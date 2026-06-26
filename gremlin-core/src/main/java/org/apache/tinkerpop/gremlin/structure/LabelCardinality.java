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
package org.apache.tinkerpop.gremlin.structure;

/**
 * Defines the label cardinality for graph elements. Each value declares its multiplicity constraints
 * via {@link #min()} and {@link #max()}, and whether mutation is permitted via {@link #supportsMutation()}.
 * <p>
 * Providers declare their supported cardinality via {@link Graph.Features}.
 * Validation of label operations against these constraints is handled by
 * {@link org.apache.tinkerpop.gremlin.structure.util.LabelCardinalityValidator}.
 *
 * @since 4.0.0
 */
public enum LabelCardinality {

    /**
     * Exactly one label, immutable. All mutation operations throw.
     * This is the default for TinkerGraph and provides backward compatibility with TinkerPop 3.x.
     */
    ONE(1, 1, false),

    /**
     * One or more labels. The element must always have at least one label.
     * {@code dropLabels()} always throws. {@code dropLabel(x)} succeeds only if at least one label remains.
     */
    ONE_OR_MORE(1, Integer.MAX_VALUE, true),

    /**
     * Zero or more labels, fully flexible. No constraints on the number of labels.
     * Elements can have any number of labels including zero.
     */
    ZERO_OR_MORE(0, Integer.MAX_VALUE, true);

    private final int min;
    private final int max;
    private final boolean mutable;

    LabelCardinality(final int min, final int max, final boolean mutable) {
        this.min = min;
        this.max = max;
        this.mutable = mutable;
    }

    /**
     * The minimum number of labels an element must have under this cardinality.
     */
    public int min() { return min; }

    /**
     * The maximum number of labels an element may have under this cardinality.
     */
    public int max() { return max; }

    /**
     * Whether this cardinality allows multiple labels on an element simultaneously.
     */
    public boolean supportsMultiLabel() { return max > 1; }

    /**
     * Whether this cardinality allows an element to have zero labels.
     */
    public boolean supportsZeroLabels() { return min == 0; }

    /**
     * Whether this cardinality allows label mutation (addLabel/dropLabel).
     */
    public boolean supportsMutation() { return mutable; }
}
