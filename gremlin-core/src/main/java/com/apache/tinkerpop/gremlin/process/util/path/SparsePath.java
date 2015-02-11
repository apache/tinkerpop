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
package com.apache.tinkerpop.gremlin.process.util.path;

import com.apache.tinkerpop.gremlin.process.Path;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SparsePath implements Path, Serializable {

    private final Map<String, Object> map = new HashMap<>();
    private Object currentObject = null;

    protected SparsePath() {

    }

    public static SparsePath make() {
        return new SparsePath();
    }

    @Override
    public Path extend(final Object object, final String... labels) {
        this.currentObject = object;
        if (labels.length > 0)
            Stream.of(labels).forEach(label -> this.map.put(label, object));
        return this;
    }

    @Override
    public void addLabel(final String label) {
        this.map.put(label, this.currentObject);
    }

    @Override
    public List<Object> objects() {
        return Collections.unmodifiableList(new ArrayList<>(this.map.values()));
    }

    @Override
    public List<Set<String>> labels() {
        final List<Set<String>> labels = new ArrayList<>();
        this.map.forEach((k, v) -> labels.add(Collections.singleton(k)));
        return Collections.unmodifiableList(labels);
    }


    public <A> A get(final String label) throws IllegalArgumentException {
        final Object object = this.map.get(label);
        if (null == object)
            throw Path.Exceptions.stepWithProvidedLabelDoesNotExist(label);
        return (A) object;
    }

    @Override
    public boolean hasLabel(final String label) {
        return this.map.containsKey(label);
    }

    @Override
    public Path clone() {
        return this;
    }

    @Override
    public int size() {
        return this.map.size();
    }
}
