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
package org.apache.tinkerpop.gremlin.structure.snapshot.format;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

/**
 * Writes the owner files of a vertex property key in the multi layout, which map vertices to the ranges of
 * vertex-property ordinals they own. Owners are dense when {@code ownerCount * 4 >= vertexCount}, see
 * {@link #isDense(long, long)}:
 * <ul>
 *     <li>dense owners: {@code owner-offsets.bin}, int64 with {@code vertexCount + 1} entries, where a vertex with no
 *     vertex properties for the key has an empty range;</li>
 *     <li>sparse owners: {@code owner-ordinals.bin}, int32 with the owning vertex ordinals in ascending order, and
 *     {@code owner-offsets.bin}, int64 with {@code ownerCount + 1} entries.</li>
 * </ul>
 * The ordinals of one vertex's vertex properties are consecutive and start at the running total, so the offsets are
 * prefix sums of the per-owner property counts. Both builders use this class, so the bytes are identical. For each
 * vertex that has at least one vertex property for the key, in ascending vertex ordinal order, call
 * {@link #owner(long, long)}. The owner count must be known when the writer is created. Instances are not
 * thread-safe.
 */
public final class OwnerWriter implements AutoCloseable {

    private final String offsetsPath;
    private final String ordinalsPath;
    private final long vertexCount;
    private final long ownerCount;
    private final boolean dense;
    private final SegmentWriter offsets;
    private final SegmentWriter ordinals;
    private long owners;
    private long nextVertex;
    private long total;
    private long lastVertex = -1;
    private List<Manifest.SegmentInfo> segments;

    private OwnerWriter(final Function<String, Path> resolver, final int keyCode, final long vertexCount,
                        final long ownerCount) {
        this.offsetsPath = SegmentPaths.vertexPropertyOwnerOffsets(keyCode);
        this.ordinalsPath = SegmentPaths.vertexPropertyOwnerOrdinals(keyCode);
        this.vertexCount = vertexCount;
        this.ownerCount = ownerCount;
        this.dense = isDense(vertexCount, ownerCount);
        this.offsets = SegmentWriter.create(resolver.apply(offsetsPath), 8);
        SegmentWriter o = null;
        try {
            if (!dense) o = SegmentWriter.create(resolver.apply(ordinalsPath), 4);
        } catch (RuntimeException e) {
            offsets.close();
            throw e;
        }
        this.ordinals = o;
        offsets.writeLong(0);
    }

    /**
     * Whether owners are dense, that is {@code ownerCount * 4 >= vertexCount}, the rule used for columns with the
     * owner count in place of the present count.
     */
    public static boolean isDense(final long vertexCount, final long ownerCount) {
        return ColumnStats.isDense(vertexCount, ownerCount);
    }

    /**
     * The owner encoding for the given counts.
     */
    public static Manifest.OwnerEncoding encodingOf(final long vertexCount, final long ownerCount) {
        return isDense(vertexCount, ownerCount) ? Manifest.OwnerEncoding.DENSE : Manifest.OwnerEncoding.SPARSE;
    }

    /**
     * Creates the writer for one vertex property key.
     *
     * @param resolver    maps a path relative to the bundle root (or the scratch root) to a file, for example
     *                    {@code BuildDirectory::segmentPath}
     * @param keyCode     the vertex property key code
     * @param vertexCount the number of vertices
     * @param ownerCount  the number of vertices that have at least one vertex property for the key
     */
    public static OwnerWriter create(final Function<String, Path> resolver, final int keyCode, final long vertexCount,
                                     final long ownerCount) {
        if (ownerCount < 0 || ownerCount > vertexCount) {
            throw new IllegalArgumentException("Key " + keyCode + " has " + ownerCount + " owners among "
                    + vertexCount + " vertices");
        }
        return new OwnerWriter(resolver, keyCode, vertexCount, ownerCount);
    }

    /**
     * Adds an owner.
     *
     * @param vertexOrdinal the owning vertex, greater than the previous owner and less than the vertex count
     * @param propertyCount the number of vertex properties the vertex has for the key, at least 1
     */
    public void owner(final long vertexOrdinal, final long propertyCount) {
        if (vertexOrdinal <= lastVertex || vertexOrdinal >= vertexCount) {
            throw new IllegalArgumentException("Owner " + vertexOrdinal + " is not in (" + lastVertex + ", "
                    + vertexCount + ")");
        }
        if (propertyCount < 1) throw new IllegalArgumentException("An owner needs at least one vertex property");
        if (owners >= ownerCount) throw new IllegalStateException("More owners than the announced " + ownerCount);
        lastVertex = vertexOrdinal;
        if (dense) {
            while (nextVertex < vertexOrdinal) {
                offsets.writeLong(total);
                nextVertex++;
            }
            nextVertex = vertexOrdinal + 1;
        } else {
            ordinals.writeInt((int) vertexOrdinal);
        }
        total += propertyCount;
        offsets.writeLong(total);
        owners++;
    }

    /**
     * The number of vertex properties added so far, which is the next vertex-property ordinal.
     */
    public long propertyCount() {
        return total;
    }

    /**
     * Completes the files.
     *
     * @param expectedPropertyCount the key's vertex-property count, checked against the sum of the owners' counts
     * @return the segments written, in ascending path order
     */
    public List<Manifest.SegmentInfo> finish(final long expectedPropertyCount) {
        if (segments != null) return segments;
        if (owners != ownerCount) {
            throw new IllegalStateException("Received " + owners + " owners, expected " + ownerCount);
        }
        if (total != expectedPropertyCount) {
            throw new IllegalStateException("Owners hold " + total + " vertex properties, expected "
                    + expectedPropertyCount);
        }
        if (dense) {
            while (nextVertex < vertexCount) {
                offsets.writeLong(total);
                nextVertex++;
            }
        }
        offsets.finish();
        final List<Manifest.SegmentInfo> out = new ArrayList<>();
        out.add(offsets.info(offsetsPath));
        if (ordinals != null) {
            ordinals.finish();
            out.add(ordinals.info(ordinalsPath));
        }
        out.sort(Comparator.comparing(Manifest.SegmentInfo::path));
        segments = out;
        return segments;
    }

    @Override
    public void close() {
        offsets.close();
        if (ordinals != null) ordinals.close();
    }
}
