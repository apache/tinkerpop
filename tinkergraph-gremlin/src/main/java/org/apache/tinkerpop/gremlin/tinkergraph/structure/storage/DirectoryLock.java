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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.StandardOpenOption;

/**
 * An exclusive, whole-process lock on a {@code TinkerStorageGraph} storage directory. A persistent, transactional
 * TinkerGraph is a single-writer embedded store: two graphs opened on the same directory — whether in one JVM or
 * across processes — would both append to and compact the same files, corrupting them. This holds an OS advisory lock
 * ({@link FileLock}) on a {@code LOCK} file in the directory for the lifetime of the graph, so a second open fails
 * fast rather than silently corrupting data.
 * <p/>
 * An OS lock (rather than a mere marker file) is used so the kernel releases it automatically if the JVM dies,
 * avoiding a stale lock that would wedge the store after a crash.
 * <p/>
 * Note: {@link FileLock} semantics are unreliable on some network filesystems (notably NFS); this guarantee holds on
 * local filesystems.
 */
public final class DirectoryLock implements AutoCloseable {

    static final String LOCK_FILE = "LOCK";

    private final FileChannel channel;
    private final FileLock lock;
    private final File lockFile;

    private DirectoryLock(final FileChannel channel, final FileLock lock, final File lockFile) {
        this.channel = channel;
        this.lock = lock;
        this.lockFile = lockFile;
    }

    /**
     * Acquire an exclusive lock on the {@code LOCK} file within {@code directory}.
     *
     * @param directory the storage directory, which must already exist
     * @return the held lock, released by {@link #close()}
     * @throws IllegalStateException if another graph (in this or another process) already holds the lock
     */
    public static DirectoryLock acquire(final File directory) {
        final File lockFile = new File(directory, LOCK_FILE);
        FileChannel channel = null;
        try {
            channel = FileChannel.open(lockFile.toPath(),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            final FileLock lock = channel.tryLock();
            if (lock == null) {
                channel.close();
                throw lockedByAnother(directory, null);
            }
            return new DirectoryLock(channel, lock, lockFile);
        } catch (OverlappingFileLockException ofle) {
            // another graph in *this* JVM already holds (or is acquiring) the lock on this file
            closeQuietly(channel);
            throw lockedByAnother(directory, ofle);
        } catch (IOException ex) {
            closeQuietly(channel);
            throw new UncheckedIOException(String.format("Could not acquire storage lock for %s", directory), ex);
        }
    }

    private static IllegalStateException lockedByAnother(final File directory, final Throwable cause) {
        return new IllegalStateException(String.format(
                "Storage location %s is already in use by another TinkerStorageGraph (in this or another process); "
                        + "a persistent TinkerStorageGraph allows only a single writer", directory), cause);
    }

    private static void closeQuietly(final FileChannel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {
                // best effort on the failure path
            }
        }
    }

    /**
     * Release the lock and close the channel. Idempotent-friendly: safe to call once per acquired lock.
     */
    @Override
    public void close() {
        try {
            if (lock.isValid())
                lock.release();
        } catch (IOException ex) {
            throw new UncheckedIOException(String.format("Could not release storage lock %s", lockFile), ex);
        } finally {
            closeQuietly(channel);
        }
    }
}
