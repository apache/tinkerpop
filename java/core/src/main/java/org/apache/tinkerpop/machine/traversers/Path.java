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
package org.apache.tinkerpop.machine.traversers;

import java.io.Serializable;
import java.util.ArrayList;
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
        path.objects.forEach(o -> objects.add(o));
        path.labels.forEach(l -> labels.add(l));
    }

    public void add(final Set<String> labels, final Object object) {
        this.labels.add(labels);
        this.objects.add(object);
    }

    public void addLabels(final Set<String> labels) {
        this.labels.get(this.labels.size() - 1).addAll(labels);
    }

    @Override
    public String toString() {
        return this.objects.toString();
    }
}
