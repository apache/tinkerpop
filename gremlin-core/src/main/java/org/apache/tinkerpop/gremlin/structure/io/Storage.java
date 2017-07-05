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

package org.apache.tinkerpop.gremlin.structure.io;

import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.List;

/**
 * Storage is a standard API that providers can implement to allow "file-system"-based access to data sources.
 * The methods provided by Storage are similar in form and behavior to standard Linux operating system commands.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Storage {

    /**
     * List all the data sources in the root directory.
     *
     * @return the data sources in the root directory
     */
    public List<String> ls();

    /**
     * List all the data sources at the specified location.
     *
     * @param location a location
     * @return the data sources at the specified location
     */
    public List<String> ls(final String location);

    /**
     * Recursively copy all the data sources from the source location to the target location.
     *
     * @param sourceLocation the source location
     * @param targetLocation the target location
     * @return whether data sources were copied
     */
    public boolean cp(final String sourceLocation, final String targetLocation);

    /**
     * Determine whether the specified location has a data source.
     *
     * @param location a location to check
     * @return whether that location has a data source.
     */
    public boolean exists(final String location);

    /**
     * Recursively remove the data source at the specified location.
     *
     * @param location the location of the data source
     * @return whether a data source was removed.
     */
    public boolean rm(final String location);

    /**
     * Get a string representation of the specified number of lines at the data source location.
     *
     * @param location the data source location
     * @return an iterator of lines
     */
    public default Iterator<String> head(final String location) {
        return this.head(location, Integer.MAX_VALUE);
    }

    /**
     * Get a string representation of the specified number of lines at the data source location.
     *
     * @param location   the data source location
     * @param totalLines the total number of lines to retrieve
     * @return an iterator of lines.
     */
    public Iterator<String> head(final String location, final int totalLines);

    /**
     * Get the vertices at the specified graph location.
     *
     * @param location    the location of the graph (or the root location and search will be made)
     * @param readerClass the class of the parser that understands the graph format
     * @param totalLines  the total number of lines of the graph to return
     * @return an iterator of vertices.
     */
    public Iterator<Vertex> head(final String location, final Class readerClass, final int totalLines);

    /**
     * Get the vertices at the specified graph location.
     *
     * @param location    the location of the graph (or the root location and search will be made)
     * @param readerClass the class of the parser that understands the graph format
     * @return an iterator of vertices.
     */
    public default Iterator<Vertex> head(final String location, final Class readerClass) {
        return this.head(location, readerClass, Integer.MAX_VALUE);
    }

    /**
     * Get the {@link KeyValue} data at the specified memory location.
     *
     * @param location    the root location of the data
     * @param memoryKey   the memory key
     * @param readerClass the class of the parser that understands the memory format
     * @param totalLines  the total number of key-values to return
     * @return an iterator of key-values.
     */
    public <K, V> Iterator<KeyValue<K, V>> head(final String location, final String memoryKey, final Class readerClass, final int totalLines);


    /**
     * Get the {@link KeyValue} data at the specified memory location.
     *
     * @param location    the root location of the data
     * @param memoryKey   the memory key
     * @param readerClass the class of the parser that understands the memory format
     * @return an iterator of key-values.
     */
    public default <K, V> Iterator<KeyValue<K, V>> head(final String location, final String memoryKey, final Class readerClass) {
        return this.head(location, memoryKey, readerClass, Integer.MAX_VALUE);
    }
}
