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
 * Writes the multi vertex label layout: {@code vertices/label-offsets.bin}, an int64 segment with one more entry than
 * there are vertices, and {@code vertices/label-codes.bin}, the label codes of all vertices in vertex order, each
 * vertex's codes in source order, narrowed to the width that fits the final dictionary (see
 * {@link LabelDictionary#codeWidth(long)}). A vertex with no labels has an empty range. Both builders use this class,
 * the materializing builder from its heap lists and the streaming builder by replaying its label spool, so the bytes
 * are identical.
 * <p/>
 * For each vertex in ascending ordinal order, call {@link #add(int)} once per label and then {@link #endVertex()}. The
 * final dictionary size must be known when the writer is created. Instances are not thread-safe.
 */
public final class LabelListWriter implements AutoCloseable {

    private final SegmentWriter offsets;
    private final SegmentWriter codes;
    private final int labelCount;
    private long vertices;
    private long total;
    private List<Manifest.SegmentInfo> segments;

    private LabelListWriter(final SegmentWriter offsets, final SegmentWriter codes, final int labelCount) {
        this.offsets = offsets;
        this.codes = codes;
        this.labelCount = labelCount;
        offsets.writeLong(0);
    }

    /**
     * Creates the writer.
     *
     * @param resolver   maps a path relative to the bundle root (or the scratch root) to a file, for example
     *                   {@code BuildDirectory::segmentPath}
     * @param labelCount the final number of vertex labels, which decides the code width
     */
    public static LabelListWriter create(final Function<String, Path> resolver, final int labelCount) {
        final SegmentWriter offsets = SegmentWriter.create(resolver.apply(SegmentPaths.VERTEX_LABEL_OFFSETS), 8);
        try {
            final SegmentWriter codes = LabelDictionary.createCodeWriter(
                    resolver.apply(SegmentPaths.VERTEX_LABEL_CODES), labelCount);
            return new LabelListWriter(offsets, codes, labelCount);
        } catch (RuntimeException e) {
            offsets.close();
            throw e;
        }
    }

    /**
     * Appends a label code of the current vertex.
     *
     * @throws IllegalArgumentException if the code is outside the dictionary
     */
    public void add(final int code) {
        if (code < 0 || code >= labelCount) {
            throw new IllegalArgumentException("Label code " + code + " is outside the dictionary of " + labelCount
                    + " labels");
        }
        LabelDictionary.writeCode(codes, code);
        total++;
    }

    /**
     * Ends the current vertex, which may have had no labels, and starts the next.
     */
    public void endVertex() {
        offsets.writeLong(total);
        vertices++;
    }

    /**
     * Completes both segments.
     *
     * @param vertexCount the number of vertices, checked against the number of {@link #endVertex()} calls
     * @return the two segments, in ascending path order
     */
    public List<Manifest.SegmentInfo> finish(final long vertexCount) {
        if (segments != null) return segments;
        if (vertices != vertexCount) {
            throw new IllegalStateException("Label lists were written for " + vertices + " vertices, expected "
                    + vertexCount);
        }
        offsets.finish();
        codes.finish();
        final List<Manifest.SegmentInfo> out = new ArrayList<>();
        out.add(offsets.info(SegmentPaths.VERTEX_LABEL_OFFSETS));
        out.add(codes.info(SegmentPaths.VERTEX_LABEL_CODES));
        out.sort(Comparator.comparing(Manifest.SegmentInfo::path));
        segments = out;
        return segments;
    }

    @Override
    public void close() {
        offsets.close();
        codes.close();
    }
}
