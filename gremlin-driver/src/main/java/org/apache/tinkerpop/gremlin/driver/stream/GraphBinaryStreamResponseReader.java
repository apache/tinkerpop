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
package org.apache.tinkerpop.gremlin.driver.stream;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.Marker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Performs pull-based streaming deserialization of a GraphBinary v4 response from an {@link InputStreamBuffer}.
 * Reads one item at a time using the {@link GraphBinaryReader} and {@code TypeSerializer} infrastructure,
 * pushing each result to the {@link ResultSet} as it is deserialized.
 * <p>
 * Wire format: {@code [version_byte][bulked_flag_byte][items...][EndOfStream marker][status_code][message][exception]}
 */
public class GraphBinaryStreamResponseReader implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(GraphBinaryStreamResponseReader.class);

    private final Buffer buffer;
    private final GraphBinaryReader reader;
    private final ResultSet resultSet;
    private final AtomicReference<ResultSet> pendingResultSet;

    public GraphBinaryStreamResponseReader(final Buffer buffer,
                                           final GraphBinaryReader reader,
                                           final ResultSet resultSet,
                                           final AtomicReference<ResultSet> pendingResultSet) {
        this.buffer = buffer;
        this.reader = reader;
        this.resultSet = resultSet;
        this.pendingResultSet = pendingResultSet;
    }

    @Override
    public void run() {
        try {
            // Read header: version byte (MSB must be 1) and bulking flag
            final byte version = buffer.readByte();
            if ((version & 0x80) == 0) {
                throw new RuntimeException("Invalid GraphBinary response version: " + version);
            }
            final boolean bulked = (buffer.readByte() & 1) == 1;

            // Read items until EndOfStream marker
            while (true) {
                final Object obj = reader.read(buffer);
                if (obj instanceof Marker) {
                    break;
                }

                if (bulked) {
                    final long bulk = reader.read(buffer);
                    resultSet.add(new Result(new DefaultRemoteTraverser<>(obj, bulk)));
                } else {
                    resultSet.add(new Result(obj));
                }
            }

            // Read footer: status code, nullable message, nullable exception
            final int statusCode = reader.readValue(buffer, Integer.class, false);
            final String message = reader.readValue(buffer, String.class, true);
            final String exception = reader.readValue(buffer, String.class, true);

            // Status code 0 means success in GraphBinary v4 — the server omits the HTTP status code
            // in the binary footer when the response is successful.
            if (statusCode == 0 || statusCode == HttpResponseStatus.OK.code()) {
                resultSet.markComplete();
            } else {
                resultSet.markError(ResponseException.create(HttpResponseStatus.valueOf(statusCode), message, exception));
            }
        } catch (Throwable t) {
            logger.warn("Error reading streaming response", t);
            resultSet.markError(t);
        } finally {
            pendingResultSet.compareAndSet(resultSet, null);
            buffer.release();
        }
    }
}
