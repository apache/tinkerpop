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

/**
 * Builds a snapshot bundle from a {@link SnapshotSource}. The build runs in a sibling directory named
 * {@code <target>.tmp-<uuid>}, which is moved to the target when complete. The build fails if the target already
 * exists. I/O failures are reported as {@link java.io.UncheckedIOException} and unsupported data as
 * {@link org.apache.tinkerpop.gremlin.structure.snapshot.spi.UnsupportedSnapshotDataException}.
 */
public interface SnapshotBuilder {

    BuildStats build(SnapshotSource source, Path target, BuildOptions options);
}
