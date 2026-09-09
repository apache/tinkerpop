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
package org.apache.tinkerpop.gremlin.tinkergraph.structure.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The set of property keys a persistent graph indexes, held in a small file beside the storage engine's own files.
 * <p/>
 * Index definitions are deliberately kept out of the transactional log. An index is purely an optimization, so losing
 * a definition to a crash costs a rebuild rather than any data: a crash between {@code createIndex} and the write here
 * drops a definition the caller can recreate, and one after {@code dropIndex} resurrects an index that is dropped
 * again. Neither can change a query result, which is what makes a file outside the write-ahead log an honest place to
 * keep this. Nothing that affects correctness may be stored this way.
 * <p/>
 * The file is line oriented and readable, one record per line: {@code V} or {@code E}, a tab, then the property key
 * with backslash, tab, newline and carriage return escaped, so a key containing any of them survives a round trip.
 */
public final class IndexDefinitions {

    private static final Logger logger = LoggerFactory.getLogger(IndexDefinitions.class);

    static final String INDEX_FILE = "INDEXES";

    private static final String VERTEX = "V";
    private static final String EDGE = "E";

    private final Set<String> vertexKeys;
    private final Set<String> edgeKeys;

    public IndexDefinitions(final Set<String> vertexKeys, final Set<String> edgeKeys) {
        this.vertexKeys = new LinkedHashSet<>(vertexKeys);
        this.edgeKeys = new LinkedHashSet<>(edgeKeys);
    }

    public Set<String> vertexKeys() {
        return vertexKeys;
    }

    public Set<String> edgeKeys() {
        return edgeKeys;
    }

    public boolean isEmpty() {
        return vertexKeys.isEmpty() && edgeKeys.isEmpty();
    }

    /**
     * Read the definitions recorded in {@code directory}, or an empty set if none have been recorded.
     * <p/>
     * A file that cannot be read is reported and treated as empty rather than raised. The graph then opens with no
     * indexes, which is exactly how it behaved before definitions were recorded at all, so an unreadable file can
     * never make a store less openable than the data it holds.
     */
    public static IndexDefinitions read(final File directory) {
        final File file = new File(directory, INDEX_FILE);
        if (!file.isFile())
            return new IndexDefinitions(new LinkedHashSet<>(), new LinkedHashSet<>());

        final Set<String> vertexKeys = new LinkedHashSet<>();
        final Set<String> edgeKeys = new LinkedHashSet<>();
        try {
            final List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);
            for (final String line : lines) {
                if (line.isEmpty() || line.charAt(0) == '#')
                    continue;
                final int tab = line.indexOf('\t');
                if (tab < 0)
                    throw new IOException("Malformed index definition line: " + line);
                final String key = unescape(line.substring(tab + 1));
                switch (line.substring(0, tab)) {
                    case VERTEX: vertexKeys.add(key); break;
                    case EDGE: edgeKeys.add(key); break;
                    default: throw new IOException("Unknown index element type in line: " + line);
                }
            }
        } catch (IOException ex) {
            logger.warn("Could not read index definitions from {}; opening with no indexes. " +
                    "Recreate them with createIndex() if they are wanted.", file, ex);
            return new IndexDefinitions(new LinkedHashSet<>(), new LinkedHashSet<>());
        }
        return new IndexDefinitions(vertexKeys, edgeKeys);
    }

    /**
     * Replace the definitions recorded in {@code directory}. Written to a temporary file, forced to the device and
     * renamed into place, so a crash leaves either the previous set or the new one and never a partial file.
     */
    public void write(final File directory) {
        final File file = new File(directory, INDEX_FILE);
        if (isEmpty()) {
            try {
                Files.deleteIfExists(file.toPath());
                syncDirectory(directory);
            } catch (IOException ex) {
                logger.warn("Could not remove index definitions file {}", file, ex);
            }
            return;
        }

        final File tmp = new File(directory, INDEX_FILE + ".tmp");
        try {
            try (final FileOutputStream fos = new FileOutputStream(tmp);
                 final Writer out = new OutputStreamWriter(fos, StandardCharsets.UTF_8)) {
                out.write("# TinkerGraph index definitions; recreated on open\n");
                for (final String key : vertexKeys)
                    out.write(VERTEX + '\t' + escape(key) + '\n');
                for (final String key : edgeKeys)
                    out.write(EDGE + '\t' + escape(key) + '\n');
                out.flush();
                fos.getFD().sync();
            }
            atomicMove(tmp, file);
            syncDirectory(directory);
        } catch (IOException ex) {
            logger.warn("Could not record index definitions in {}; they will not survive a reopen", file, ex);
        }
    }

    private static String escape(final String key) {
        final StringBuilder sb = new StringBuilder(key.length());
        for (int i = 0; i < key.length(); i++) {
            final char c = key.charAt(i);
            switch (c) {
                case '\\': sb.append("\\\\"); break;
                case '\t': sb.append("\\t"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                default: sb.append(c);
            }
        }
        return sb.toString();
    }

    private static String unescape(final String value) throws IOException {
        final StringBuilder sb = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            final char c = value.charAt(i);
            if (c != '\\') {
                sb.append(c);
                continue;
            }
            if (++i == value.length())
                throw new IOException("Index definition ends with a dangling escape: " + value);
            switch (value.charAt(i)) {
                case '\\': sb.append('\\'); break;
                case 't': sb.append('\t'); break;
                case 'n': sb.append('\n'); break;
                case 'r': sb.append('\r'); break;
                default: throw new IOException("Unknown escape in index definition: " + value);
            }
        }
        return sb.toString();
    }

    private static void atomicMove(final File source, final File target) throws IOException {
        try {
            Files.move(source.toPath(), target.toPath(),
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException anse) {
            Files.move(source.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void syncDirectory(final File directory) {
        try (final FileChannel dirChannel = FileChannel.open(directory.toPath(), StandardOpenOption.READ)) {
            dirChannel.force(true);
        } catch (IOException ex) {
            // some platforms (notably Windows) cannot open a directory as a channel; the atomic rename is the
            // durability guarantee there, so treat inability to sync the directory as non-fatal
        }
    }
}
