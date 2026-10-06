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
 * Thrown when a native operator cannot reserve the memory it needs and has no spill path left. It names the operator and
 * its reservation so the failure can be traced to one piece of state.
 */
public final class CsrMemoryBudgetException extends RuntimeException {

    private final String owner;
    private final long requestedBytes;
    private final long ownerReservedBytes;
    private final long reservedBytes;
    private final long limitBytes;

    public CsrMemoryBudgetException(final String owner, final long requestedBytes, final long ownerReservedBytes,
                                    final long reservedBytes, final long limitBytes) {
        this(owner, requestedBytes, ownerReservedBytes, reservedBytes, limitBytes, null);
    }

    /**
     * @param largestHolder the owners that hold the most bytes, with their bytes, for the message; null to leave it out
     */
    public CsrMemoryBudgetException(final String owner, final long requestedBytes, final long ownerReservedBytes,
                                    final long reservedBytes, final long limitBytes, final String largestHolder) {
        super("Memory budget exceeded: limit " + limitBytes + " bytes, reserved " + reservedBytes + " bytes, requested "
                + requestedBytes + " bytes by " + owner + " (which holds " + ownerReservedBytes + " bytes)"
                + (largestHolder == null ? "" : "; the largest holders are " + largestHolder));
        this.owner = owner;
        this.requestedBytes = requestedBytes;
        this.ownerReservedBytes = ownerReservedBytes;
        this.reservedBytes = reservedBytes;
        this.limitBytes = limitBytes;
    }

    /**
     * The operator that asked for the memory.
     */
    public String owner() {
        return owner;
    }

    public long requestedBytes() {
        return requestedBytes;
    }

    /**
     * The bytes the owner already held when the request failed.
     */
    public long ownerReservedBytes() {
        return ownerReservedBytes;
    }

    /**
     * The bytes reserved by all owners when the request failed.
     */
    public long reservedBytes() {
        return reservedBytes;
    }

    public long limitBytes() {
        return limitBytes;
    }
}
