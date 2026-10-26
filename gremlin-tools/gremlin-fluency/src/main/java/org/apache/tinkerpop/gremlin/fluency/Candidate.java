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
package org.apache.tinkerpop.gremlin.fluency;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * One attempt at writing the traversal for an {@link IntentScenario}. A {@code null} traversal means the driver
 * produced no answer.
 */
public final class Candidate {
    /**
     * A line that starts a traversal. An answer without one is prose, however it is fenced or annotated.
     */
    private static final Pattern TRAVERSAL_LINE = Pattern.compile("(?m)^\\s*g\\s*\\.");

    private final IntentScenario scenario;
    private final String attemptId;
    private final String traversal;

    public Candidate(final IntentScenario scenario, final String attemptId, final String traversal) {
        this.scenario = Objects.requireNonNull(scenario, "scenario must not be null");
        this.attemptId = Objects.requireNonNull(attemptId, "attemptId must not be null");
        if (attemptId.contains("\n"))
            throw new IllegalArgumentException("attemptId must be a single line");
        this.traversal = traversal;
    }

    public IntentScenario getScenario() {
        return scenario;
    }

    /**
     * Identifies the attempt within a run. The copied scenario is named after it.
     */
    public String getAttemptId() {
        return attemptId;
    }

    public String getTraversal() {
        return traversal;
    }

    public boolean hasAnswer() {
        return traversal != null && !traversal.trim().isEmpty();
    }

    /**
     * Whether the answer has a line that starts a traversal. An answer without one declined to write a traversal,
     * so there is nothing to score.
     */
    public boolean hasTraversal() {
        return hasAnswer() && TRAVERSAL_LINE.matcher(traversal).find();
    }
}
