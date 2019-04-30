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
package org.apache.tinkerpop.machine.traverser.path;

import org.apache.tinkerpop.machine.structure.TPair;

import java.util.Collections;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class EmptyPath implements Path {

    private static final EmptyPath INSTANCE = new EmptyPath();

    private EmptyPath() {

    }

    @Override
    public boolean has(final String key) {
        return false;
    }

    @Override
    public void add(final String label, final Object object) {

    }

    @Override
    public void remove(final String key) {

    }

    @Override
    public void remove(final TPair<String, Object> pair) {

    }

    @Override
    public Iterator<TPair<String, Object>> iterator() {
        return Collections.emptyIterator();
    }

    @Override
    public Object object(int index) {
        throw new IllegalStateException("No objects in EmptyPath");
    }

    @Override
    public String label(int index) {
        return null;
    }

    @Override
    public Object get(final Pop pop, final String label) {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public Path clone() {
        return INSTANCE;
    }

    public static final EmptyPath instance() {
        return INSTANCE;
    }
}
