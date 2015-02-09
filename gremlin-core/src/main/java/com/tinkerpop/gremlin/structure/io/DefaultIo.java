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
package com.tinkerpop.gremlin.structure.io;

import com.tinkerpop.gremlin.structure.Graph;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Default implementation of the {@link Graph.Io} interface which overrides none of the default methods.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DefaultIo implements Graph.Io {
    private final Graph g;

    public DefaultIo(final Graph g) {
        this.g = g;
    }

    @Override
    public void writeKryo(final String file) throws IOException {
        try (final OutputStream out = new FileOutputStream(file)) {
            kryoWriter().create().writeGraph(out, g);
        }
    }

    @Override
    public void readKryo(final String file) throws IOException {
        try (final InputStream in = new FileInputStream(file)) {
            kryoReader().create().readGraph(in, g);
        }
    }

    @Override
    public void writeGraphML(final String file) throws IOException {
        try (final OutputStream out = new FileOutputStream(file)) {
            graphMLWriter().create().writeGraph(out, g);
        }
    }

    @Override
    public void readGraphML(final String file) throws IOException {
        try (final InputStream in = new FileInputStream(file)) {
            graphMLReader().create().readGraph(in, g);
        }
    }

    @Override
    public void writeGraphSON(final String file) throws IOException {
        try (final OutputStream out = new FileOutputStream(file)) {
            graphSONWriter().create().writeGraph(out, g);
        }
    }

    @Override
    public void readGraphSON(final String file) throws IOException {
        try (final InputStream in = new FileInputStream(file)) {
            graphSONReader().create().readGraph(in, g);
        }
    }
}
