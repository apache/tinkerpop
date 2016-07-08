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
package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import org.apache.tinkerpop.gremlin.process.traversal.Path;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class EmptyPath implements Path, Serializable {

    private static final EmptyPath INSTANCE = new EmptyPath();

    private EmptyPath() {

    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public Path extend(final Object object, final Set<String> labels) {
        return this;
    }

    @Override
    public Path extend(final Set<String> labels) {
        return this;
    }

    @Override
    public Path retract(final Set<String> labels) { return this; }

    @Override
    public <A> A get(final String label) {
        throw Path.Exceptions.stepWithProvidedLabelDoesNotExist(label);
    }

    @Override
    public <A> A get(final int index) {
        return (A) Collections.emptyList().get(index);
    }

    @Override
    public boolean hasLabel(final String label) {
        return false;
    }

    @Override
    public List<Object> objects() {
        return Collections.emptyList();
    }

    @Override
    public List<Set<String>> labels() {
        return Collections.emptyList();
    }

    @Override
    public boolean isSimple() {
        return true;
    }

    @Override
    @SuppressWarnings("CloneDoesntCallSuperClone,CloneDoesntDeclareCloneNotSupportedException")
    public EmptyPath clone() {
        return this;
    }

    public static Path instance() {
        return INSTANCE;
    }

    @Override
    public int hashCode() {
        return -1424379551;
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof EmptyPath;
    }
}
