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
package org.apache.tinkerpop.gremlin.structure.snapshot.spi;

import java.util.Objects;

/**
 * Identity and version of a snapshot source, both of which are recorded in the manifest.
 */
public final class SourceVersion {

    private final String sourceId;
    private final String version;

    public SourceVersion(final String sourceId, final String version) {
        this.sourceId = Objects.requireNonNull(sourceId);
        this.version = Objects.requireNonNull(version);
    }

    public String sourceId() {
        return sourceId;
    }

    public String version() {
        return version;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof SourceVersion)) return false;
        final SourceVersion other = (SourceVersion) o;
        return sourceId.equals(other.sourceId) && version.equals(other.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceId, version);
    }

    @Override
    public String toString() {
        return "SourceVersion{sourceId=" + sourceId + ", version=" + version + "}";
    }
}
