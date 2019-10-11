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
package org.apache.tinkerpop.gremlin.structure.io.binary;

import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;

import java.io.IOException;

/**
 * This is a dummy implementation of {@link Io} which is only used in the context of helping to configure a
 * GraphBinary {@code MessageSerializer} with an {@link IoRegistry}. It's methods are not implemented.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphBinaryIo implements Io {
    @Override
    public GraphReader.ReaderBuilder reader() {
        throw new UnsupportedOperationException("GraphBinaryIo is only used to support IoRegistry configuration - it's methods are not implemented");
    }

    @Override
    public GraphWriter.WriterBuilder writer() {
        throw new UnsupportedOperationException("GraphBinaryIo is only used to support IoRegistry configuration - it's methods are not implemented");
    }

    @Override
    public Mapper.Builder mapper() {
        throw new UnsupportedOperationException("GraphBinaryIo is only used to support IoRegistry configuration - it's methods are not implemented");
    }

    @Override
    public void writeGraph(final String file) throws IOException {
        throw new UnsupportedOperationException("GraphBinaryIo is only used to support IoRegistry configuration - it's methods are not implemented");
    }

    @Override
    public void readGraph(final String file) throws IOException {
        throw new UnsupportedOperationException("GraphBinaryIo is only used to support IoRegistry configuration - it's methods are not implemented");
    }
}
