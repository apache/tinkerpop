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
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/**
 * File operations shared by the files a {@code TinkerStorageGraph} keeps in its storage directory, which are all
 * replaced whole by writing a temporary file and renaming it into place.
 */
final class StorageFiles {

    private static final Logger logger = LoggerFactory.getLogger(StorageFiles.class);

    private StorageFiles() {
    }

    /**
     * Atomically move {@code source} onto {@code target}, replacing any existing target. Falls back to a non-atomic
     * replacing move on filesystems that do not support atomic moves.
     */
    static void atomicMove(final File source, final File target) throws IOException {
        try {
            Files.move(source.toPath(), target.toPath(),
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException anse) {
            Files.move(source.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * fsync {@code directory} so that recent namespace changes (a rename into place, a file deletion) are durable.
     */
    static void syncDirectory(final File directory) {
        final FileChannel dirChannel;
        try {
            dirChannel = FileChannel.open(directory.toPath(), StandardOpenOption.READ);
        } catch (IOException ex) {
            // some platforms (notably Windows) cannot open a directory as a channel; the atomic rename is the
            // durability guarantee there, so treat inability to sync the directory as non-fatal
            return;
        }
        // some filesystems (certain network and FUSE mounts) open a directory but refuse to force it, so a failure is
        // reported rather than fatal: the rename has happened, and only its durability against a crash is in doubt
        try (final FileChannel channel = dirChannel) {
            channel.force(true);
        } catch (IOException ex) {
            logger.warn("Could not force storage directory {} to disk, so a file just renamed into it may not survive " +
                    "a crash", directory, ex);
        }
    }

    /**
     * Delete {@code file} if it exists, ignoring a failure. For temporary files that a later write overwrites anyway.
     */
    static void deleteQuietly(final File file) {
        try {
            Files.deleteIfExists(file.toPath());
        } catch (IOException ignored) {
            // best effort: the next write of the same temporary file overwrites it
        }
    }
}
