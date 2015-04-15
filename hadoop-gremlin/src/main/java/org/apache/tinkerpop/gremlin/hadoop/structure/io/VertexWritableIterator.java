/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.hadoop.structure.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.tinkerpop.gremlin.hadoop.structure.hdfs.HiddenFileFilter;
import org.apache.tinkerpop.gremlin.process.traversal.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexWritableIterator implements Iterator<Vertex> {

    private final VertexWritable value = new VertexWritable();
    private boolean available = false;
    private final Queue<SequenceFile.Reader> readers = new LinkedList<>();

    public VertexWritableIterator(final Configuration configuration, final Path path) throws IOException {
        final FileSystem fs = FileSystem.get(configuration);
        for (final FileStatus status : fs.listStatus(path, HiddenFileFilter.instance())) {
            this.readers.add(new SequenceFile.Reader(fs, status.getPath(), configuration));
        }
    }

    @Override
    public boolean hasNext() {
        try {
            if (this.available) {
                return true;
            } else {
                while (true) {
                    if (this.readers.isEmpty())
                        return false;
                    if (this.readers.peek().next(this.value)) {
                        this.available = true;
                        return true;
                    } else
                        this.readers.remove();
                }
            }
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public Vertex next() {
        try {
            if (this.available) {
                this.available = false;
                return this.value.get();
            } else {
                while (true) {
                    if (this.readers.isEmpty())
                        throw FastNoSuchElementException.instance();
                    if (this.readers.peek().next(this.value)) {
                        return this.value.get();
                    } else
                        this.readers.remove();
                }
            }
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}