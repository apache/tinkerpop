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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.UUID;

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

    private static final Logger logger = LoggerFactory.getLogger(DirectoryLock.class);

    static final String LOCK_FILE = "LOCK";

    private final FileChannel channel;
    private final FileLock lock;
    private final File lockFile;

    /**
     * Whether acquiring the lock created the {@code LOCK} file, rather than finding it left by an earlier graph.
     */
    private final boolean createdFile;

    private DirectoryLock(final FileChannel channel, final FileLock lock, final File lockFile,
                          final boolean createdFile) {
        this.channel = channel;
        this.lock = lock;
        this.lockFile = lockFile;
        this.createdFile = createdFile;
    }

    /**
     * Acquire an exclusive lock on the {@code LOCK} file within {@code directory}.
     *
     * @param directory the storage directory, which must already exist
     * @return the held lock, released by {@link #close()}
     * @throws IllegalStateException if another graph (in this or another process) already holds the lock
     */
    public static DirectoryLock acquire(final File directory) {
        return acquire(directory, true);
    }

    private static DirectoryLock acquire(final File directory, final boolean verify) {
        final File lockFile = new File(directory, LOCK_FILE);
        FileChannel channel = null;
        final boolean createdFile = !lockFile.exists();
        try {
            channel = FileChannel.open(lockFile.toPath(),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            final FileLock lock = channel.tryLock();
            if (lock == null) {
                channel.close();
                throw lockedByAnother(directory, null);
            }
            if (verify && !stillAtPath(channel, lockFile)) {
                lock.release();
                channel.close();
                throw lockedByAnother(directory, null);
            }
            return new DirectoryLock(channel, lock, lockFile, createdFile);
        } catch (OverlappingFileLockException ofle) {
            // another graph in *this* JVM already holds (or is acquiring) the lock on this file
            closeQuietly(channel);
            throw lockedByAnother(directory, ofle);
        } catch (IOException ex) {
            closeQuietly(channel);
            throw new UncheckedIOException(String.format("Could not acquire storage lock for %s", directory), ex);
        }
    }

    /**
     * Lock {@code directory} for a recovery open, which only reads it. A writable directory is locked exclusively,
     * as by {@link #acquire}. A directory the process cannot write, such as a read-only mount or copy of a damaged
     * store, can hold no new lock file: an existing one is locked shared, which still fails while a writer holds it,
     * and with none the directory is read unlocked, since recovery writes nothing to it.
     *
     * @param directory the storage directory to lock
     * @return the held lock
     * @throws IllegalStateException if another graph holds the lock
     */
    public static DirectoryLock acquireForRecovery(final File directory) {
        // recovery leaves every file as it found it, so it does not write the token that verifies the lock, and since
        // it never writes to the store, a lock it holds on a file that was replaced cannot cost a commit
        if (Files.isWritable(directory.toPath()))
            return acquire(directory, false);
        final File lockFile = new File(directory, LOCK_FILE);
        if (!lockFile.exists()) {
            logger.warn("Storage directory {} is not writable, so it is read for recovery without a lock; make sure " +
                    "no other graph writes to it", directory);
            return new DirectoryLock(null, null, lockFile, false);
        }
        FileChannel channel = null;
        try {
            channel = FileChannel.open(lockFile.toPath(), StandardOpenOption.READ);
            final FileLock lock = channel.tryLock(0L, Long.MAX_VALUE, true);
            if (lock == null) {
                channel.close();
                throw lockedByAnother(directory, null);
            }
            return new DirectoryLock(channel, lock, lockFile, false);
        } catch (OverlappingFileLockException ofle) {
            closeQuietly(channel);
            throw lockedByAnother(directory, ofle);
        } catch (IOException ex) {
            closeQuietly(channel);
            throw new UncheckedIOException(String.format("Could not acquire storage lock for %s", directory), ex);
        }
    }

    /**
     * Whether {@code lockFile} still names the file {@code channel} holds. A graph whose open fails removes the LOCK file
     * it created, so between opening that file and locking it, another graph could have unlinked it and created a new
     * one at the path, leaving this lock on a file that keeps no one out. A random token written through the channel
     * and read back through the path tells the two apart, which comparing file keys cannot, since the channel does not
     * report the key of the file it has open.
     */
    private static boolean stillAtPath(final FileChannel channel, final File lockFile) throws IOException {
        final UUID token = UUID.randomUUID();
        final byte[] bytes = ByteBuffer.allocate(2 * Long.BYTES)
                .putLong(token.getMostSignificantBits()).putLong(token.getLeastSignificantBits()).array();
        channel.truncate(0);
        channel.write(ByteBuffer.wrap(bytes), 0);
        channel.force(false);
        try {
            return Arrays.equals(bytes, Files.readAllBytes(lockFile.toPath()));
        } catch (NoSuchFileException ex) {
            return false;
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
     * Release the lock and, if acquiring it created the {@code LOCK} file, delete that file, leaving the directory as
     * it was found. For a graph that must not change the directory: one whose open failed, or one opened for recovery.
     */
    public void closeAndRemoveIfCreated() {
        // delete while the lock is still held where the platform allows it, so no other graph can take the lock on this
        // file in between and then have its LOCK file deleted under it. Windows refuses to delete an open file, so
        // there the file is removed once the lock is released.
        final boolean deleted = createdFile && lockFile.delete();
        close();
        if (createdFile && !deleted && lockFile.exists() && !lockFile.delete())
            throw new UncheckedIOException(new IOException(String.format("Could not remove storage lock %s",
                    lockFile)));
    }

    /**
     * Release the lock and close the channel. Idempotent-friendly: safe to call once per acquired lock.
     */
    @Override
    public void close() {
        // a recovery open of a directory it cannot write holds no lock at all
        if (null == channel)
            return;
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
