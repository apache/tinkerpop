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

package org.apache.tinkerpop.gremlin.hadoop.structure.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.tinkerpop.gremlin.hadoop.structure.hdfs.HDFSTools;
import org.apache.tinkerpop.gremlin.hadoop.structure.hdfs.HiddenFileFilter;
import org.apache.tinkerpop.gremlin.hadoop.structure.hdfs.TextIterator;
import org.apache.tinkerpop.gremlin.structure.io.Storage;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class FileSystemStorage implements Storage {

    private static final String SPACE = " ";
    private static final String D_SPACE = "(D) ";

    private final FileSystem fs;

    public FileSystemStorage(final FileSystem fileSystem) {
        this.fs = fileSystem;
    }

    private static String fileStatusString(final FileStatus status) {
        StringBuilder s = new StringBuilder();
        s.append(status.getPermission()).append(" ");
        s.append(status.getOwner()).append(SPACE);
        s.append(status.getGroup()).append(SPACE);
        s.append(status.getLen()).append(SPACE);
        if (status.isDir())
            s.append(D_SPACE);
        s.append(status.getPath().getName());
        return s.toString();
    }

    @Override
    public List<String> ls() {
        return this.ls("/");
    }

    @Override
    public List<String> ls(final String location) {
        try {
            final String newLocation;
            newLocation = location.equals("/") ? this.fs.getHomeDirectory().toString() : location;
            return Stream.of(this.fs.globStatus(new Path(newLocation + "/*"))).map(FileSystemStorage::fileStatusString).collect(Collectors.toList());
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public boolean mkdir(final String location) {
        try {
            return this.fs.mkdirs(new Path(location));
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public boolean cp(final String fromLocation, final String toLocation) {
        try {
            return FileUtil.copy(this.fs, new Path(fromLocation), this.fs, new Path(toLocation), false, new Configuration());
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public boolean exists(final String location) {
        try {
            return this.fs.exists(new Path(location));
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public boolean rm(final String location) {
        try {
            return HDFSTools.globDelete(this.fs, location, false);
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public boolean rmr(final String location) {
        try {
            return HDFSTools.globDelete(this.fs, location, true);
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public <V> Iterator<V> head(final String location, final int totalLines, final Class<V> objectClass) {
        return headMaker(this.fs, location, totalLines, (Class<? extends Writable>) objectClass);
    }

    @Override
    public String toString() {
        return StringFactory.storageString(this.fs.toString());
    }

    private static Iterator headMaker(final FileSystem fs, final String path, final int totalLines, final Class<? extends Writable> writableClass) {
        try {
            if (writableClass.equals(ObjectWritable.class))
                return IteratorUtils.limit(new ObjectWritableIterator(fs.getConf(), new Path(path)), totalLines);
            else if (writableClass.equals(VertexWritable.class))
                return IteratorUtils.limit(new VertexWritableIterator(fs.getConf(), new Path(path)), totalLines);
            else
                return IteratorUtils.limit(new TextIterator(fs.getConf(), new Path(path)), totalLines);
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /////////

    public void copyToLocal(final String fromLocation, final String toLocation) {
        try {
            this.fs.copyToLocalFile(new Path(fromLocation), new Path(toLocation));
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public void copyFromLocal(final String fromLocation, final String toLocation) {
        try {
            this.fs.copyFromLocalFile(new Path(fromLocation), new Path(toLocation));
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public void mergeToLocal(final String fromLocation, final String toLocation) {
        try {
            final FileSystem local = FileSystem.getLocal(new Configuration());
            final FSDataOutputStream outA = local.create(new Path(toLocation));
            for (final Path path : HDFSTools.getAllFilePaths(fs, new Path(fromLocation), HiddenFileFilter.instance())) {
                final FSDataInputStream inA = fs.open(path);
                IOUtils.copyBytes(inA, outA, 8192);
                inA.close();
            }
            outA.close();
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
