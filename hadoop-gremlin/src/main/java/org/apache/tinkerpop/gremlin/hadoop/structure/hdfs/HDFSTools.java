/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.tinkerpop.gremlin.hadoop.structure.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HDFSTools {

    private static final String FORWARD_SLASH = "/";
    private static final String FORWARD_ASTERISK = "/*";

    protected HDFSTools() {
    }

    public static long getFileSize(final FileSystem fs, final Path path, final PathFilter filter) throws IOException {
        long totalSize = 0l;
        for (final Path p : getAllFilePaths(fs, path, filter)) {
            totalSize = totalSize + fs.getFileStatus(p).getLen();
        }
        return totalSize;
    }

    public static List<Path> getAllFilePaths(final FileSystem fs, Path path, final PathFilter filter) throws IOException {
        if (null == path) path = fs.getHomeDirectory();
        if (path.toString().equals(FORWARD_SLASH)) path = new Path("");

        final List<Path> paths = new ArrayList<Path>();
        if (fs.isFile(path))
            paths.add(path);
        else {
            for (final FileStatus status : fs.globStatus(new Path(path + FORWARD_ASTERISK), filter)) {
                final Path next = status.getPath();
                paths.addAll(getAllFilePaths(fs, next, filter));
            }
        }
        return paths;
    }


    public static void decompressPath(final FileSystem fs, final String in, final String out, final String compressedFileSuffix, final boolean deletePrevious) throws IOException {
        final Path inPath = new Path(in);

        if (fs.isFile(inPath))
            HDFSTools.decompressFile(fs, in, out, deletePrevious);
        else {
            final Path outPath = new Path(out);
            if (!fs.exists(outPath))
                fs.mkdirs(outPath);
            for (final Path path : FileUtil.stat2Paths(fs.globStatus(new Path(in + FORWARD_ASTERISK)))) {
                if (path.getName().endsWith(compressedFileSuffix))
                    HDFSTools.decompressFile(fs, path.toString(), outPath.toString() + FORWARD_SLASH + path.getName().split("\\.")[0], deletePrevious);
            }
        }
    }

    public static void decompressFile(final FileSystem fs, final String inFile, final String outFile, boolean deletePrevious) throws IOException {
        final Path inPath = new Path(inFile);
        final Path outPath = new Path(outFile);
        final CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        final CompressionCodec codec = factory.getCodec(inPath);
        final OutputStream out = fs.create(outPath);
        final InputStream in = codec.createInputStream(fs.open(inPath));
        IOUtils.copyBytes(in, out, 8192);
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);

        if (deletePrevious)
            fs.delete(new Path(inFile), true);

    }

    public static boolean globDelete(final FileSystem fs, final String path, final boolean recursive) throws IOException {
        boolean deleted = false;
        for (final Path p : FileUtil.stat2Paths(fs.globStatus(new Path(path)))) {
            fs.delete(p, recursive);
            deleted = true;
        }
        return deleted;
    }
}
