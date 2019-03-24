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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class BasicPath implements Path {

    private List<Set<String>> labels = new ArrayList<>();
    private List<Object> objects = new ArrayList<>();

    public BasicPath() {
    }

    @Override
    public void add(final Set<String> labels, final Object object) {
        this.labels.add(labels);
        this.objects.add(object);
    }

    @Override
    public Object object(final int index) {
        return this.objects.get(index);
    }

    @Override
    public Set<String> labels(final int index) {
        return this.labels.get(index);
    }

    @Override
    public Object get(final Pop pop, final String label) {
        if (Pop.last == pop) {
            for (int i = this.labels.size() - 1; i >= 0; i--) {
                if (this.labels.get(i).contains(label))
                    return this.objects.get(i);
            }
        } else if (Pop.all == pop) {
            final List<Object> objects = new ArrayList<>();
            for (int i = 0; i < this.labels.size(); i++) {
                if (this.labels.get(i).contains(label))
                    objects.add(this.objects.get(i));
            }
            return objects;
        } else { // must be Pop.first
            for (int i = 0; i < this.labels.size(); i++) {
                if (this.labels.get(i).contains(label))
                    return this.objects.get(i);
            }
        }
        throw Path.Exceptions.noObjectsForLabel(label);
    }

    @Override
    public int size() {
        return this.objects.size();
    }

    @Override
    public int hashCode() {
        return this.labels.hashCode() ^ this.objects.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof BasicPath &&
                this.labels.equals(((BasicPath) object).labels) &&
                this.objects.equals(((BasicPath) object).objects);
    }

    @Override
    public String toString() {
        return this.objects.toString();
    }

    @Override
    public Path clone() {
        try {
            final BasicPath clone = (BasicPath) super.clone();
            clone.objects = new ArrayList<>(this.objects);
            clone.labels = new ArrayList<>(this.labels.size());
            for (final Set<String> labelSet : this.labels) {
                clone.labels.add(new LinkedHashSet<>(labelSet));
            }
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
