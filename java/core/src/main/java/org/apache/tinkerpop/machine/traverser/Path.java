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
package org.apache.tinkerpop.machine.traverser;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Path implements Serializable {

    private final List<Object> objects = new ArrayList<>();
    private final List<Set<String>> labels = new ArrayList<>();

    public Path() {
    }

    public Path(final Path path) {
        this.objects.addAll(path.objects);
        this.labels.addAll(path.labels);
    }

    public void add(final Set<String> labels, final Object object) {
        this.labels.add(labels);
        this.objects.add(object);
    }

    public void addLabels(final Set<String> labels) {
        if (this.labels.isEmpty())
            this.labels.add(new HashSet<>());
        this.labels.get(this.labels.size() - 1).addAll(labels);
    }

    public Object object(final int index) {
        return this.objects.get(index);
    }

    public Set<String> labels(final int index) {
        return this.labels.get(index);
    }

    public int size() {
        return this.objects.size();
    }

    @Override
    public String toString() {
        return this.objects.toString();
    }
}
