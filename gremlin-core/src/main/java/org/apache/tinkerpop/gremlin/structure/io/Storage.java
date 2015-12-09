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
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Storage {

    public List<String> ls();

    public List<String> ls(final String location);

    public boolean mkdir(final String location);

    public boolean cp(final String fromLocation, final String toLocation);

    public boolean exists(final String location);

    public boolean rm(final String location);

    public boolean rmr(final String location);

    public Iterator<String> head(final String location, final int totalLines);

    public default Iterator<String> head(final String location) {
        return this.head(location, Integer.MAX_VALUE);
    }

    public Iterator<Vertex> head(final String location, final Class parserClass, final int totalLines);

    public default Iterator<Vertex> head(final String location, final Class parserClass) {
        return this.head(location, parserClass, Integer.MAX_VALUE);
    }

    public <K, V> Iterator<KeyValue<K, V>> head(final String location, final String memoryKey, final Class parserClass, final int totalLines);

    public default <K, V> Iterator<KeyValue<K, V>> head(final String location, final String memoryKey, final Class parserClass) {
        return this.head(location, memoryKey, parserClass, Integer.MAX_VALUE);
    }
}
