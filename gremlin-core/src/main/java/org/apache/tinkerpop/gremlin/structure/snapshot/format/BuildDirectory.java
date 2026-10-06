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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A snapshot under construction. A build runs in a sibling directory named {@code <target>.tmp-<uuid>}, so the final
 * move stays on one filesystem. {@link #publish(Manifest)} deletes the scratch directory, writes the manifest last,
 * and moves the build directory to the target with {@code ATOMIC_MOVE}, falling back to a plain move when the
 * filesystem does not support it. The target must not exist, both when the build starts and when it is published.
 * <p/>
 * Scratch files default to {@code <build>/scratch}. When a scratch directory is supplied, for example from
 * {@code BuildOptions.scratchDirectory()}, a unique subdirectory is created inside it and only that subdirectory is
 * ever deleted. Either way {@link #deleteScratch()} and {@link #publish(Manifest)} remove it.
 * <p/>
 * Use in a try-with-resources block: {@link #close()} deletes the build and scratch directories unless the build was
 * published, so a failed build leaves nothing behind. I/O failures are reported as {@link UncheckedIOException}.
 */
public final class BuildDirectory implements AutoCloseable {

    private static final String TEMP_SUFFIX = ".tmp-";

    private final Path target;
    private final Path buildDir;
    private final Path scratchDir;
    private boolean published;

    private BuildDirectory(final Path target, final Path buildDir, final Path scratchDir) {
        this.target = target;
        this.buildDir = buildDir;
        this.scratchDir = scratchDir;
    }

    /**
     * Starts a build with scratch space inside the build directory.
     *
     * @param target the final snapshot directory, which must not exist
     * @throws UncheckedIOException wrapping {@link FileAlreadyExistsException} if the target exists
     */
    public static BuildDirectory create(final Path target) {
        return create(target, null);
    }

    /**
     * Starts a build.
     *
     * @param target           the final snapshot directory, which must not exist
     * @param scratchDirectory a directory to hold scratch files, or null for {@code <build>/scratch}. It is created
     *                         if missing and is not deleted; a unique subdirectory of it is used instead.
     * @throws UncheckedIOException wrapping {@link FileAlreadyExistsException} if the target exists
     */
    public static BuildDirectory create(final Path target, final Path scratchDirectory) {
        final Path absolute = target.toAbsolutePath().normalize();
        try {
            if (Files.exists(absolute)) throw new FileAlreadyExistsException(absolute.toString());
            final Path parent = absolute.getParent();
            if (parent != null) Files.createDirectories(parent);
            final String id = UUID.randomUUID().toString();
            final Path buildDir = absolute.resolveSibling(absolute.getFileName() + TEMP_SUFFIX + id);
            Files.createDirectory(buildDir);
            final Path scratch;
            try {
                if (scratchDirectory == null) {
                    scratch = buildDir.resolve(SegmentPaths.SCRATCH_DIR);
                    Files.createDirectory(scratch);
                } else {
                    Files.createDirectories(scratchDirectory);
                    scratch = Files.createDirectory(scratchDirectory.toAbsolutePath()
                            .resolve(absolute.getFileName() + ".scratch-" + id));
                }
            } catch (IOException | RuntimeException e) {
                deleteRecursively(buildDir);
                throw e;
            }
            return new BuildDirectory(absolute, buildDir, scratch);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * The final snapshot directory.
     */
    public Path target() {
        return target;
    }

    /**
     * The temporary build directory, the root of the bundle while it is being built.
     */
    public Path path() {
        return buildDir;
    }

    /**
     * The scratch directory. It no longer exists after {@link #deleteScratch()}.
     */
    public Path scratch() {
        return scratchDir;
    }

    /**
     * Resolves a bundle-relative path, using {@code '/'} separators, against the build directory and creates its
     * parent directories.
     */
    public Path segmentPath(final String relativePath) {
        return resolveCreatingParents(buildDir, relativePath);
    }

    /**
     * Resolves a scratch-relative path, using {@code '/'} separators, against the scratch directory and creates its
     * parent directories.
     */
    public Path scratchPath(final String relativePath) {
        return resolveCreatingParents(scratchDir, relativePath);
    }

    /**
     * The total size in bytes of all files currently under the build directory and the scratch directory. Counts the
     * apparent file length, so a sparse mapped segment is counted at its full size. Intended for
     * {@code BuildStats} phase records.
     */
    public long bytesOnDisk() {
        final AtomicLong total = new AtomicLong();
        sumSizes(buildDir, total);
        if (!scratchDir.startsWith(buildDir)) sumSizes(scratchDir, total);
        return total.get();
    }

    /**
     * Deletes the scratch directory and everything in it. Idempotent.
     */
    public void deleteScratch() {
        deleteRecursively(scratchDir);
    }

    /**
     * Completes the build: deletes the scratch directory, writes {@code manifest.json} last, and moves the build
     * directory to the target, atomically where the filesystem supports it.
     *
     * @return the target path
     * @throws UncheckedIOException wrapping {@link FileAlreadyExistsException} if the target now exists
     */
    public Path publish(final Manifest manifest) {
        if (published) throw new IllegalStateException("Already published: " + target);
        deleteScratch();
        ManifestIO.write(manifest, buildDir.resolve(SegmentPaths.MANIFEST));
        try {
            if (Files.exists(target)) throw new FileAlreadyExistsException(target.toString());
            try {
                Files.move(buildDir, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(buildDir, target);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        published = true;
        return target;
    }

    /**
     * Deletes the build and scratch directories unless the build was published. Idempotent.
     */
    @Override
    public void close() {
        if (published) return;
        deleteScratch();
        deleteRecursively(buildDir);
    }

    private static Path resolveCreatingParents(final Path root, final String relativePath) {
        Path p = root;
        for (final String part : relativePath.split(SegmentPaths.SEPARATOR)) {
            if (part.isEmpty() || part.equals(".") || part.equals("..")) {
                throw new IllegalArgumentException("Invalid relative path: " + relativePath);
            }
            p = p.resolve(part);
        }
        try {
            final Path parent = p.getParent();
            if (parent != null) Files.createDirectories(parent);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return p;
    }

    private static void sumSizes(final Path dir, final AtomicLong total) {
        if (!Files.exists(dir)) return;
        try {
            Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) {
                    total.addAndGet(attrs.size());
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(final Path file, final IOException exc) {
                    // a file may be deleted while walking; it no longer uses space
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Deletes a file or directory tree. A missing path is not an error.
     */
    static void deleteRecursively(final Path root) {
        try {
            Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                    Files.deleteIfExists(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
                    if (exc != null) throw exc;
                    Files.deleteIfExists(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (NoSuchFileException e) {
            // already gone
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
