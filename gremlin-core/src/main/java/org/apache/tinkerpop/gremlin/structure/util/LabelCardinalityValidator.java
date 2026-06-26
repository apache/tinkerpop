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
package org.apache.tinkerpop.gremlin.structure.util;

import org.apache.tinkerpop.gremlin.structure.LabelCardinality;

import java.util.HashSet;
import java.util.Set;

/**
 * Validates label operations against a {@link LabelCardinality} constraint. All methods are stateless and static.
 * <p>
 * This class is separated from the enum so that cardinality remains a pure data descriptor while validation
 * logic can be extended, overridden by providers, or tested independently.
 *
 * @since 4.0.0
 */
public final class LabelCardinalityValidator {

    private LabelCardinalityValidator() {}

    /**
     * Validates that adding labels would not violate cardinality constraints.
     *
     * @throws IllegalStateException if mutation is not supported or max would be exceeded
     */
    public static void validateAdd(final LabelCardinality cardinality, final Set<String> currentLabels,
                                   final String label, final String... moreLabels) {
        if (!cardinality.supportsMutation())
            throw new IllegalStateException("Label mutation is not supported with cardinality " + cardinality +
                    ". Labels are immutable once assigned.");
        if (currentLabels.size() + 1 + moreLabels.length > cardinality.max())
            throw new IllegalStateException("Cannot add label(s): would result in " +
                    (currentLabels.size() + 1 + moreLabels.length) + " labels but cardinality " +
                    cardinality + " allows at most " + cardinality.max() + ".");
    }

    /**
     * Validates that dropping specific labels would not violate cardinality constraints.
     * Simulates the removal to check whether the minimum label count would be violated.
     *
     * @throws IllegalStateException if mutation is not supported or min would be violated
     */
    public static void validateDrop(final LabelCardinality cardinality, final Set<String> currentLabels,
                                    final String label, final String... moreLabels) {
        if (!cardinality.supportsMutation())
            throw new IllegalStateException("Label mutation is not supported with cardinality " + cardinality +
                    ". Labels are immutable once assigned.");
        if (cardinality.min() > 0) {
            final Set<String> result = new HashSet<>(currentLabels);
            result.remove(label);
            for (final String l : moreLabels) {
                result.remove(l);
            }
            if (result.size() < cardinality.min())
                throw new IllegalStateException("Cannot drop label(s): would leave " + result.size() +
                        " labels but cardinality " + cardinality + " requires at least " + cardinality.min() + ".");
        }
    }

    /**
     * Validates that dropping all labels would not violate cardinality constraints.
     *
     * @throws IllegalStateException if mutation is not supported or min &gt; 0
     */
    public static void validateDropAll(final LabelCardinality cardinality, final Set<String> currentLabels) {
        if (!cardinality.supportsMutation())
            throw new IllegalStateException("Label mutation is not supported with cardinality " + cardinality +
                    ". Labels are immutable once assigned.");
        if (cardinality.min() > 0)
            throw new IllegalStateException("Cannot drop all labels: cardinality " + cardinality +
                    " requires at least " + cardinality.min() + " label(s).");
    }

    /**
     * Validates that a set of labels is valid for element creation under this cardinality.
     *
     * @throws IllegalStateException if the label count violates min or max
     */
    public static void validateCreation(final LabelCardinality cardinality, final Set<String> labels) {
        if (labels.size() < cardinality.min())
            throw new IllegalStateException("Element creation requires at least " + cardinality.min() +
                    " label(s) with cardinality " + cardinality + ", got " + labels.size());
        if (labels.size() > cardinality.max())
            throw new IllegalStateException("Element creation allows at most " + cardinality.max() +
                    " label(s) with cardinality " + cardinality + ", got " + labels.size());
    }
}
