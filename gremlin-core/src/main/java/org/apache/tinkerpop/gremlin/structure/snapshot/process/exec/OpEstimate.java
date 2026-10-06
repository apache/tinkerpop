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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.exec;

/**
 * A plan-time estimate at one point of a plan: how many entries flow and how much heap the operators so far hold.
 * Estimators registered with {@link CsrOperatorFactory#registerEstimator} transform the estimate of the input into the
 * estimate of the output; the default passes the entries through and adds no memory.
 *
 * @param entries     the estimated number of entries, not counting bulk
 * @param memoryBytes the estimated bytes held by state so far
 */
public record OpEstimate(long entries, long memoryBytes) {

    public OpEstimate {
        if (entries < 0 || memoryBytes < 0) throw new IllegalArgumentException("Estimates are not negative");
    }

    public OpEstimate withEntries(final long entries) {
        return new OpEstimate(entries, memoryBytes);
    }

    public OpEstimate plusMemory(final long bytes) {
        return new OpEstimate(entries, memoryBytes + bytes);
    }
}
