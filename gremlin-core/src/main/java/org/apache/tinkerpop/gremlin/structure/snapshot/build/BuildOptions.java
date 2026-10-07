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
package org.apache.tinkerpop.gremlin.structure.snapshot.build;

import org.apache.tinkerpop.gremlin.structure.snapshot.format.SnapshotLayout;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable options for a {@link SnapshotBuilder}. Use {@link #defaults()} or {@link #builder()}.
 */
public final class BuildOptions {

    public static final long DEFAULT_MEMORY_BUDGET_BYTES = 256L * 1024 * 1024;

    private final SnapshotLayout layout;
    private final long memoryBudgetBytes;
    private final Path scratchDirectory;
    private final boolean groupedFastPath;
    private final boolean edgeIdIndex;

    private BuildOptions(final Builder builder) {
        this.layout = builder.layout;
        this.memoryBudgetBytes = builder.memoryBudgetBytes;
        this.scratchDirectory = builder.scratchDirectory;
        this.groupedFastPath = builder.groupedFastPath;
        this.edgeIdIndex = builder.edgeIdIndex;
    }

    public static BuildOptions defaults() {
        return new Builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Which segments are published. Defaults to {@link SnapshotLayout#FULL}.
     */
    public SnapshotLayout layout() {
        return layout;
    }

    /**
     * Bytes the streaming builder may use for sort chunks and buffers, and the heap that the hybrid builder may fill with
     * spools, lookup maps, sort arrays and adjacency arrays before it spills to scratch. Defaults to
     * {@link #DEFAULT_MEMORY_BUDGET_BYTES}.
     */
    public long memoryBudgetBytes() {
        return memoryBudgetBytes;
    }

    /**
     * Directory for scratch files, or empty to use the temporary build directory.
     */
    public Optional<Path> scratchDirectory() {
        return Optional.ofNullable(scratchDirectory);
    }

    /**
     * Whether the builder may use the grouped-by-out-vertex fast path when the source reports
     * {@code GROUPED_BY_OUT_VERTEX}. Defaults to true; false lets the generic path be measured against a grouped source.
     */
    public boolean groupedFastPath() {
        return groupedFastPath;
    }

    /**
     * Whether the edge identifier index is published for layouts that include identity. Defaults to true.
     */
    public boolean edgeIdIndex() {
        return edgeIdIndex;
    }

    public static final class Builder {
        private SnapshotLayout layout = SnapshotLayout.FULL;
        private long memoryBudgetBytes = DEFAULT_MEMORY_BUDGET_BYTES;
        private Path scratchDirectory;
        private boolean groupedFastPath = true;
        private boolean edgeIdIndex = true;

        private Builder() {
        }

        public Builder layout(final SnapshotLayout layout) {
            this.layout = Objects.requireNonNull(layout);
            return this;
        }

        public Builder memoryBudgetBytes(final long memoryBudgetBytes) {
            if (memoryBudgetBytes <= 0) throw new IllegalArgumentException("memoryBudgetBytes must be positive");
            this.memoryBudgetBytes = memoryBudgetBytes;
            return this;
        }

        public Builder scratchDirectory(final Path scratchDirectory) {
            this.scratchDirectory = scratchDirectory;
            return this;
        }

        public Builder groupedFastPath(final boolean groupedFastPath) {
            this.groupedFastPath = groupedFastPath;
            return this;
        }

        public Builder edgeIdIndex(final boolean edgeIdIndex) {
            this.edgeIdIndex = edgeIdIndex;
            return this;
        }

        public BuildOptions build() {
            return new BuildOptions(this);
        }
    }
}
