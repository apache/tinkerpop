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

/**
 * Relative paths of the segments in a snapshot bundle. All paths use {@code '/'} as the separator and are relative to
 * the bundle root. Directories named by property-key code use the code's decimal form.
 */
public final class SegmentPaths {

    private SegmentPaths() {
    }

    public static final String SEPARATOR = "/";

    public static final String MANIFEST = "manifest.json";

    /**
     * Scratch directory under the build directory for spools and anything the layout does not publish.
     */
    public static final String SCRATCH_DIR = "scratch";

    // file extension of every segment
    public static final String SEGMENT_EXTENSION = ".bin";

    // top-level directories
    public static final String VERTICES_DIR = "vertices";
    public static final String EDGES_DIR = "edges";
    public static final String ADJACENCY_DIR = "adjacency";
    public static final String PROPERTIES_DIR = "properties";

    // vertices/ and edges/
    public static final String IDS_DIR = "ids";
    public static final String LABELS = "labels.bin";
    public static final String ID_INDEX_KEYS = "id-index-keys.bin";
    public static final String ID_INDEX_ORDINALS = "id-index-ordinals.bin";
    public static final String OUT_VERTICES = "out-vertices.bin";
    public static final String IN_VERTICES = "in-vertices.bin";

    public static final String VERTEX_IDS_DIR = VERTICES_DIR + SEPARATOR + IDS_DIR;
    public static final String VERTEX_LABELS = VERTICES_DIR + SEPARATOR + LABELS;
    // multi vertex label layout: int64 offsets (V + 1) into narrowed label codes
    public static final String LABEL_OFFSETS = "label-offsets.bin";
    public static final String LABEL_CODES = "label-codes.bin";
    public static final String VERTEX_LABEL_OFFSETS = VERTICES_DIR + SEPARATOR + LABEL_OFFSETS;
    public static final String VERTEX_LABEL_CODES = VERTICES_DIR + SEPARATOR + LABEL_CODES;
    public static final String VERTEX_ID_INDEX_KEYS = VERTICES_DIR + SEPARATOR + ID_INDEX_KEYS;
    public static final String VERTEX_ID_INDEX_ORDINALS = VERTICES_DIR + SEPARATOR + ID_INDEX_ORDINALS;

    public static final String EDGE_IDS_DIR = EDGES_DIR + SEPARATOR + IDS_DIR;
    public static final String EDGE_LABELS = EDGES_DIR + SEPARATOR + LABELS;
    public static final String EDGE_OUT_VERTICES = EDGES_DIR + SEPARATOR + OUT_VERTICES;
    public static final String EDGE_IN_VERTICES = EDGES_DIR + SEPARATOR + IN_VERTICES;
    public static final String EDGE_ID_INDEX_KEYS = EDGES_DIR + SEPARATOR + ID_INDEX_KEYS;
    public static final String EDGE_ID_INDEX_ORDINALS = EDGES_DIR + SEPARATOR + ID_INDEX_ORDINALS;

    // adjacency/
    public static final String OUT_OFFSETS = ADJACENCY_DIR + SEPARATOR + "out-offsets.bin";
    public static final String OUT_NEIGHBORS = ADJACENCY_DIR + SEPARATOR + "out-neighbors.bin";
    public static final String OUT_EDGES = ADJACENCY_DIR + SEPARATOR + "out-edges.bin";
    public static final String IN_OFFSETS = ADJACENCY_DIR + SEPARATOR + "in-offsets.bin";
    public static final String IN_NEIGHBORS = ADJACENCY_DIR + SEPARATOR + "in-neighbors.bin";
    public static final String IN_EDGES = ADJACENCY_DIR + SEPARATOR + "in-edges.bin";

    // properties/
    public static final String VERTEX_PROPERTIES_DIR = PROPERTIES_DIR + SEPARATOR + "vertex";
    public static final String EDGE_PROPERTIES_DIR = PROPERTIES_DIR + SEPARATOR + "edge";

    // multi vertex-property layout, inside properties/vertex/<keyCode>/
    public static final String OWNER_OFFSETS = "owner-offsets.bin";
    public static final String OWNER_ORDINALS = "owner-ordinals.bin";
    public static final String META_DIR = "meta";

    // graph variables
    public static final String VARIABLES_DIR = "variables";

    // files inside a column directory
    public static final String PRESENCE = "presence.bin";
    public static final String ORDINALS = "ordinals.bin";
    public static final String VALUES = "values.bin";
    public static final String OFFSETS = "offsets.bin";
    public static final String DATA = "data.bin";
    public static final String TYPES = "types.bin";
    public static final String NULLS = "nulls.bin";

    /**
     * Directory of the column for a vertex property key, for example {@code properties/vertex/3}.
     */
    public static String vertexPropertyDir(final int keyCode) {
        return VERTEX_PROPERTIES_DIR + SEPARATOR + keyCode;
    }

    /**
     * Directory of the vertex-property identifier column for a key, for example {@code properties/vertex/3/ids}.
     */
    public static String vertexPropertyIdsDir(final int keyCode) {
        return vertexPropertyDir(keyCode) + SEPARATOR + IDS_DIR;
    }

    /**
     * Path of {@code owner-offsets.bin} of a multi-layout vertex property key.
     */
    public static String vertexPropertyOwnerOffsets(final int keyCode) {
        return file(vertexPropertyDir(keyCode), OWNER_OFFSETS);
    }

    /**
     * Path of {@code owner-ordinals.bin} of a multi-layout vertex property key with sparse owners.
     */
    public static String vertexPropertyOwnerOrdinals(final int keyCode) {
        return file(vertexPropertyDir(keyCode), OWNER_ORDINALS);
    }

    /**
     * Directory of a meta-property column, for example {@code properties/vertex/3/meta/1}.
     */
    public static String vertexPropertyMetaDir(final int keyCode, final int metaKeyCode) {
        return vertexPropertyDir(keyCode) + SEPARATOR + META_DIR + SEPARATOR + metaKeyCode;
    }

    /**
     * Directory of the column for an edge property key, for example {@code properties/edge/3}.
     */
    public static String edgePropertyDir(final int keyCode) {
        return EDGE_PROPERTIES_DIR + SEPARATOR + keyCode;
    }

    /**
     * Path of a file inside a column directory.
     */
    public static String file(final String directory, final String fileName) {
        return directory + SEPARATOR + fileName;
    }
}
