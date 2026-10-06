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

import java.nio.file.Path;

/**
 * The execution settings of one {@code CsrSuperStep}, resolved by the strategy from the graph configuration and
 * {@code OptionsStrategy} and immutable afterwards.
 *
 * @param memoryBudgetBytes the limit of the {@link MemoryBudget}
 * @param scratchDirectory  the directory under which spill files are created, or null for the system temporary
 *                          directory
 * @param batchSize         the capacity of batches
 */
public record CsrSettings(long memoryBudgetBytes, Path scratchDirectory, int batchSize) {

    /**
     * The budget used when nothing is configured, 256 MiB.
     */
    public static final long DEFAULT_MEMORY_BUDGET = 256L * 1024 * 1024;

    public static final CsrSettings DEFAULT = new CsrSettings(DEFAULT_MEMORY_BUDGET, null, Batch.DEFAULT_CAPACITY);

    public CsrSettings {
        if (memoryBudgetBytes < 0) throw new IllegalArgumentException("The memory budget must not be negative");
        if (batchSize < 1) throw new IllegalArgumentException("The batch size must be positive");
    }

    public CsrSettings withMemoryBudget(final long bytes) {
        return new CsrSettings(bytes, scratchDirectory, batchSize);
    }
}
