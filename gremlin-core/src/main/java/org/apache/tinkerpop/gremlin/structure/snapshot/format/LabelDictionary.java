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

import org.apache.tinkerpop.gremlin.structure.snapshot.spi.UnsupportedSnapshotDataException;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Assigns dense codes to strings in order of first appearance. It is used for the vertex and edge label dictionaries
 * and, as separate instances, for the vertex and edge property-key dictionaries. The code of a string is its index in
 * {@link #labels()}, which is exactly the list stored in the {@code Manifest}.
 * <p/>
 * A {@code labels.bin} segment stores one code per element in the narrowest unsigned width that fits the final
 * dictionary: 1 byte for at most 256 labels, 2 bytes for at most 65,536 and 4 bytes otherwise. The width is only known
 * when the scan is complete, so a builder either holds the codes in heap and writes them with
 * {@link #createCodeWriter(Path, int)}, or spools them as int32 and converts the spool with
 * {@link #narrow(Path, Path, String, int)}. Both produce identical bytes.
 * <p/>
 * Instances are not thread-safe.
 */
public final class LabelDictionary {

    private final Map<String, Integer> codes = new HashMap<>();
    private final List<String> labels = new ArrayList<>();

    /**
     * The code of the string, assigning the next free code if it has not been seen.
     *
     * @throws UnsupportedSnapshotDataException if the string is null
     */
    public int codeOf(final String label) {
        final Integer code = codes.get(label);
        if (code != null) return code;
        if (label == null) throw new UnsupportedSnapshotDataException("Null label or property key is not supported");
        final int assigned = labels.size();
        codes.put(label, assigned);
        labels.add(label);
        return assigned;
    }

    /**
     * The number of strings seen so far.
     */
    public int size() {
        return labels.size();
    }

    public String label(final int code) {
        return labels.get(code);
    }

    /**
     * The strings in code order, as stored in the manifest. The view is not a copy and grows as codes are assigned.
     */
    public List<String> labels() {
        return Collections.unmodifiableList(labels);
    }

    /**
     * The width in bytes of the stored codes for the current size.
     */
    public int codeWidth() {
        return codeWidth(labels.size());
    }

    /**
     * The width in bytes of stored codes for a dictionary of {@code labelCount} labels: 1, 2 or 4.
     */
    public static int codeWidth(final long labelCount) {
        if (labelCount <= 1L << 8) return 1;
        if (labelCount <= 1L << 16) return 2;
        return 4;
    }

    /**
     * Creates a writer for a {@code labels.bin} segment sized for a dictionary of {@code labelCount} labels. Append
     * codes with {@link #writeCode(SegmentWriter, int)}.
     */
    public static SegmentWriter createCodeWriter(final Path path, final int labelCount) {
        return SegmentWriter.create(path, codeWidth(labelCount));
    }

    /**
     * Appends a code to a writer created by {@link #createCodeWriter(Path, int)}.
     */
    public static void writeCode(final SegmentWriter writer, final int code) {
        switch (writer.valueWidth()) {
            case 1:
                writer.writeByte(code);
                break;
            case 2:
                writer.writeShort(code);
                break;
            default:
                writer.writeInt(code);
                break;
        }
    }

    /**
     * Reads the code at {@code index} of a mapped {@code labels.bin}.
     */
    public static int readCode(final MappedSegment segment, final long index) {
        switch (segment.valueWidth()) {
            case 1:
                return segment.getByte(index) & 0xff;
            case 2:
                return segment.getShort(index) & 0xffff;
            default:
                return segment.getInt(index);
        }
    }

    /**
     * Reads the next code from a sequential reader of a {@code labels.bin}.
     */
    public static int readCode(final SegmentReader reader) {
        switch (reader.valueWidth()) {
            case 1:
                return reader.readByte() & 0xff;
            case 2:
                return reader.readShort() & 0xffff;
            default:
                return reader.readInt();
        }
    }

    /**
     * Converts a spool of int32 codes into a {@code labels.bin} with the narrowest width for the final dictionary size,
     * in one sequential pass.
     *
     * @param spool        an int32 segment of label codes, one per element
     * @param output       where to write the narrowed segment
     * @param relativePath the segment's bundle-relative path for the returned info
     * @param labelCount   the final number of labels, which decides the width
     * @return the finished segment, ready for the manifest
     * @throws IllegalStateException if the spool contains a code outside {@code [0, labelCount)}
     */
    public static Manifest.SegmentInfo narrow(final Path spool, final Path output, final String relativePath,
                                              final int labelCount) {
        try (SegmentReader in = SegmentReader.open(spool);
             SegmentWriter out = createCodeWriter(output, labelCount)) {
            final long n = in.count();
            for (long i = 0; i < n; i++) {
                final int code = in.readInt();
                if (code < 0 || code >= labelCount) {
                    throw new IllegalStateException("Label code " + code + " at index " + i + " of " + spool
                            + " is outside the dictionary of " + labelCount + " labels");
                }
                writeCode(out, code);
            }
            out.finish();
            return out.info(relativePath);
        }
    }
}
