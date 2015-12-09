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

import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TextIterator implements Iterator<String> {

    private String line;
    private boolean available = false;
    private final Queue<BufferedReader> readers = new LinkedList<>();

    public TextIterator(final Configuration configuration, final Path path) throws IOException {
        final FileSystem fs = FileSystem.get(configuration);
        for (final FileStatus status : fs.listStatus(path, HiddenFileFilter.instance())) {
            this.readers.add(new BufferedReader(new InputStreamReader(fs.open(status.getPath()))));
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
                    if ((this.line = this.readers.peek().readLine()) != null) {
                        this.available = true;
                        return true;
                    } else
                        this.readers.remove().close();
                }
            }
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public String next() {
        try {
            if (this.available) {
                this.available = false;
                return this.line;
            } else {
                while (true) {
                    if (this.readers.isEmpty())
                        throw FastNoSuchElementException.instance();
                    if ((this.line = this.readers.peek().readLine()) != null) {
                        return this.line;
                    } else
                        this.readers.remove().close();
                }
            }
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
