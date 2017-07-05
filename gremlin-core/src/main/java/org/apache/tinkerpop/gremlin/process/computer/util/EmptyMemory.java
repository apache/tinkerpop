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

package org.apache.tinkerpop.gremlin.process.computer.util;

import org.apache.tinkerpop.gremlin.process.computer.Memory;

import java.util.Collections;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class EmptyMemory implements Memory.Admin {

    private static final EmptyMemory INSTANCE = new EmptyMemory();

    private EmptyMemory() {

    }

    public static EmptyMemory instance() {
        return INSTANCE;
    }

    @Override
    public void setIteration(final int iteration) {
        throw Memory.Exceptions.memoryIsCurrentlyImmutable();
    }

    @Override
    public void setRuntime(final long runtime) {
        throw Memory.Exceptions.memoryIsCurrentlyImmutable();
    }

    @Override
    public Memory asImmutable() {
        return this;
    }

    @Override
    public Set<String> keys() {
        return Collections.emptySet();
    }

    @Override
    public <R> R get(final String key) throws IllegalArgumentException {
        throw Memory.Exceptions.memoryDoesNotExist(key);
    }

    @Override
    public void set(final String key, final Object value) throws IllegalArgumentException, IllegalStateException {
        throw Memory.Exceptions.memoryIsCurrentlyImmutable();
    }

    @Override
    public void add(final String key, final Object value) throws IllegalArgumentException, IllegalStateException {
        throw Memory.Exceptions.memoryIsCurrentlyImmutable();
    }

    @Override
    public int getIteration() {
        return 0;
    }

    @Override
    public long getRuntime() {
        return 0;
    }

    @Override
    public boolean exists(final String key) {
        return false;
    }
}
