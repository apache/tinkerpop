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

/**
 * A {@link Candidate} with the {@link Outcome} of running it.
 */
public final class ScoredAttempt {
    private final Candidate candidate;
    private final Outcome outcome;
    private final String detail;
    private final DifferentialOutcome differential;
    private final String differentialDetail;

    ScoredAttempt(final Candidate candidate, final Outcome outcome, final String detail) {
        this(candidate, outcome, detail, null, null);
    }

    private ScoredAttempt(final Candidate candidate, final Outcome outcome, final String detail,
                          final DifferentialOutcome differential, final String differentialDetail) {
        this.candidate = candidate;
        this.outcome = outcome;
        this.detail = detail;
        this.differential = differential;
        this.differentialDetail = differentialDetail;
    }

    ScoredAttempt withDifferential(final DifferentialOutcome differential, final String differentialDetail) {
        return new ScoredAttempt(candidate, outcome, detail, differential, differentialDetail);
    }

    public Candidate getCandidate() {
        return candidate;
    }

    public Outcome getOutcome() {
        return outcome;
    }

    /**
     * The message of the failure behind the outcome, or {@code null} for a pass.
     */
    public String getDetail() {
        return detail;
    }

    /**
     * What the differential pass found, or {@code null} if the attempt did not pass or was not checked by it.
     */
    public DifferentialOutcome getDifferential() {
        return differential;
    }

    /**
     * Why the differential pass found what it did, or {@code null} when it agrees.
     */
    public String getDifferentialDetail() {
        return differentialDetail;
    }

    @Override
    public String toString() {
        return candidate.getAttemptId() + " " + outcome + (null == detail ? "" : " - " + detail) +
                (null == differential ? "" : " " + differential) +
                (null == differentialDetail ? "" : " - " + differentialDetail);
    }
}
