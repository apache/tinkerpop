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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.value;

import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ColumnCursor;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ColumnReader;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.OwnerCursor;
import org.apache.tinkerpop.gremlin.structure.snapshot.graph.CsrGraph;

import java.util.List;
import java.util.Set;
import java.util.Collections;
import java.util.LinkedHashSet;

/**
 * The property keys an operator reads, resolved against the snapshot, with one cursor per key for finding the
 * vertex-property or edge-property range of an owner. Keys are in argument order (all keys in key-code order when none
 * were requested); negative codes are dropped. After {@link #seek(int, int)}, {@link #start()} and {@link #end()} are
 * the half-open range of the key for the owner: vertex-property ordinals for a vertex, where the single layout's
 * ordinal is the entry index of the value column, and the one entry index of the value column for an edge.
 */
final class KeyRanges {

    private final boolean vertices;
    private final int[] codes;
    private final String[] names;
    private final ColumnReader[] columns;
    private final OwnerCursor[] owners;
    private final ColumnCursor[] cursors;
    private long start;
    private long end;

    KeyRanges(final CsrGraph graph, final boolean vertices, final int[] requested) {
        this.vertices = vertices;
        final CsrSnapshot snapshot = graph.snapshot();
        if (requested.length == 0) {
            final int count = vertices ? graph.vertexKeyCount() : graph.edgeKeyCount();
            codes = new int[count];
            for (int i = 0; i < count; i++) codes[i] = i;
        } else {
            int n = 0;
            final int[] kept = new int[requested.length];
            for (final int code : requested) {
                if (code >= 0) kept[n++] = code;
            }
            codes = java.util.Arrays.copyOf(kept, n);
        }
        names = new String[codes.length];
        columns = new ColumnReader[codes.length];
        owners = vertices ? new OwnerCursor[codes.length] : null;
        cursors = vertices ? null : new ColumnCursor[codes.length];
        for (int k = 0; k < codes.length; k++) {
            names[k] = vertices ? graph.vertexKey(codes[k]) : graph.edgeKey(codes[k]);
            columns[k] = vertices ? snapshot.vertexPropertyColumn(codes[k]) : graph.edgeColumn(codes[k]);
            if (vertices) owners[k] = snapshot.vertexPropertyCursor(codes[k]);
            else cursors[k] = columns[k].cursor();
        }
    }

    int size() {
        return codes.length;
    }

    int code(final int k) {
        return codes[k];
    }

    String name(final int k) {
        return names[k];
    }

    ColumnReader column(final int k) {
        return columns[k];
    }

    /**
     * Positions on the range of key {@code k} for the vertex or edge.
     *
     * @return true if the range is not empty
     */
    boolean seek(final int k, final int owner) {
        if (vertices) {
            if (owners[k].seek(owner)) {
                start = owners[k].start();
                end = owners[k].end();
                return true;
            }
        } else {
            final long entry = cursors[k].seek(owner);
            if (entry >= 0) {
                start = entry;
                end = entry + 1;
                return true;
            }
        }
        start = 0;
        end = 0;
        return false;
    }

    long start() {
        return start;
    }

    long end() {
        return end;
    }

    /**
     * The labels of a vertex as the facade reports them: the empty set, a singleton, or the distinct labels in source
     * order.
     */
    static Set<String> vertexLabels(final CsrSnapshot snapshot, final int vertex) {
        final int count = snapshot.vertexLabelCount(vertex);
        if (count == 0) return Collections.emptySet();
        final List<String> dictionary = snapshot.vertexLabels();
        if (count == 1) return Collections.singleton(dictionary.get(snapshot.vertexLabelCodeAt(vertex, 0)));
        final Set<String> labels = new LinkedHashSet<>();
        for (int i = 0; i < count; i++) labels.add(dictionary.get(snapshot.vertexLabelCodeAt(vertex, i)));
        return Collections.unmodifiableSet(labels);
    }
}
