/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.groovy.plugin;

/**
 * A software artifact identified by its maven coordinates.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Artifact {
    private final String group;
    private final String artifact;
    private final String version;

    /**
     * Create a new instance.
     *
     * @param group the {@code groupId}
     * @param artifact the {@code artifactId}
     * @param version the {@code version}
     */
    public Artifact(final String group, final String artifact, final String version) {
        if (group == null || group.isEmpty())
            throw new IllegalArgumentException("group cannot be null or empty");

        if (artifact == null || artifact.isEmpty())
            throw new IllegalArgumentException("artifact cannot be null or empty");

        if (version == null || version.isEmpty())
            throw new IllegalArgumentException("version cannot be null or empty");

        this.group = group;
        this.artifact = artifact;
        this.version = version;
    }

    public String getGroup() {
        return group;
    }

    public String getArtifact() {
        return artifact;
    }

    public String getVersion() {
        return version;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Artifact a = (Artifact) o;

        if (group != null ? !group.equals(a.group) : a.group != null) return false;
        if (artifact != null ? !artifact.equals(a.artifact) : a.artifact != null) return false;
        if (version != null ? !version.equals(a.version) : a.version != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = group != null ? group.hashCode() : 0;
        result = 31 * result + (artifact != null ? artifact.hashCode() : 0);
        result = 31 * result + (version != null ? version.hashCode() : 0);
        return result;
    }
}