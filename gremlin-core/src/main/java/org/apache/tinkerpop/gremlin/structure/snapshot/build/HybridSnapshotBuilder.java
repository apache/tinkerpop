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

import org.apache.tinkerpop.gremlin.structure.snapshot.spi.SnapshotSource;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Builds a snapshot bundle with the construction steps of the {@link StreamingSnapshotBuilder}, but keeps each
 * structure in heap for as long as {@link BuildOptions#memoryBudgetBytes()} allows and only spills to scratch files
 * when the budget runs low. In heap are, in order of preference, the identifier lookup map used to resolve edge
 * endpoints, the degree arrays and fill cursors, the spools of identifiers, labels, index keys and properties, the sort
 * of the index keys, and the adjacency arrays that are filled before they are written sequentially. When a reservation
 * does not fit the largest spool in heap is moved to its file first; a structure that still does not fit takes the
 * streaming builder's route (external sort and mapped index, mapped arrays, file spools). With a budget too small for
 * anything the build is the streaming build.
 * <p/>
 * The budget counts the large structures only; the dictionaries, buffers and the objects of the source are extra. The
 * output is byte-for-byte that of the heap and streaming builders for the same source scan order.
 */
public final class HybridSnapshotBuilder implements SnapshotBuilder {

    @Override
    public BuildStats build(final SnapshotSource source, final Path target, final BuildOptions options) {
        Objects.requireNonNull(source);
        Objects.requireNonNull(target);
        Objects.requireNonNull(options);
        return new StreamingSnapshotBuilder(true).build(source, target, options);
    }
}
