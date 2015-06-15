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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ImmutablePath implements Path, ImmutablePathImpl, Serializable, Cloneable {

    private ImmutablePathImpl previousPath = HeadPath.instance();
    private Object currentObject;
    private Set<String> currentLabels = new LinkedHashSet<>();

    protected ImmutablePath() {

    }

    public static Path make() {
        return HeadPath.instance();
    }

    @SuppressWarnings("CloneDoesntCallSuperClone,CloneDoesntDeclareCloneNotSupportedException")
    @Override
    public ImmutablePath clone() {
        return this;
    }

    private ImmutablePath(final Object currentObject, final Set<String> currentLabels) {
        this(HeadPath.instance(), currentObject, currentLabels);
    }

    private ImmutablePath(final ImmutablePathImpl previousPath, final Object currentObject, final Set<String> currentLabels) {
        this.previousPath = previousPath;
        this.currentObject = currentObject;
        this.currentLabels.addAll(currentLabels);
    }

    @Override
    public int size() {
        return this.previousPath.size() + 1;
    }

    @Override
    public Path extend(final Object object, final Set<String> labels) {
        return new ImmutablePath(this, object, labels);
    }

    @Override
    public <A> A get(final int index) {
        return (this.size() - 1) == index ? (A) this.currentObject : this.previousPath.get(index);
    }

    @Override
    public <A> List<A> getList(final String label) {
        // Recursively build the list to avoid building objects/labels collections.
        final List<A> list = this.previousPath.getList(label);
        // Add our object, if our step labels match.
        if (this.currentLabels.contains(label))
            list.add((A) currentObject);
        return list;
    }

    @Override
    public <A> A getSingleHead(final String label) {
        // Recursively search for the single value to avoid building throwaway collections, and to stop looking when we
        // find it.
        A single;
        // See if we have a value.
        if (this.currentLabels.contains(label)) {
            single = (A) this.currentObject;
        } else {
            // Look for a previous value.
            single = this.previousPath.getSingleHead(label);
        }
        return single;
    }

    @Override
    public <A> A getSingleTail(final String label) {
        // Recursively search for the single value to avoid building throwaway collections, and to stop looking when we
        // find it.
        A single = this.previousPath.getSingleTail(label);
        if (null == single) {
            // See if we have a value.
            if (this.currentLabels.contains(label)) {
                single = (A) this.currentObject;
            }
        }
        return single;
    }

    @Override
    public boolean hasLabel(final String label) {
        return this.currentLabels.contains(label) || this.previousPath.hasLabel(label);
    }

    @Override
    public void addLabel(final String label) {
        this.currentLabels.add(label);
    }

    @Override
    public List<Object> objects() {
        final List<Object> objectPath = new ArrayList<>();
        objectPath.addAll(this.previousPath.objects());
        objectPath.add(this.currentObject);
        return Collections.unmodifiableList(objectPath);
    }

    @Override
    public List<Set<String>> labels() {
        final List<Set<String>> labelPath = new ArrayList<>();
        labelPath.addAll(this.previousPath.labels());
        labelPath.add(this.currentLabels);
        return Collections.unmodifiableList(labelPath);
    }

    @Override
    public String toString() {
        return this.objects().toString();
    }

    private static class HeadPath implements Path, ImmutablePathImpl {
        private static final HeadPath INSTANCE = new HeadPath();

        private HeadPath() {

        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public Path extend(final Object object, final Set<String> labels) {
            return new ImmutablePath(object, labels);
        }

        @Override
        public <A> A get(final String label) {
            throw Path.Exceptions.stepWithProvidedLabelDoesNotExist(label);
        }

        @Override
        public <A> A get(final int index) {
            return (A) Collections.emptyList().get(index);
        }

        @Override
        public <A> List<A> getList(final String label) {
            // Provide an empty, modifiable list as a seed for ImmutablePath.getList.
            return new ArrayList<A>();
        }

        @Override
        public <A> A getSingleHead(final String label) {
            // Provide a null to bounce the search back to ImmutablePath.getSingleHead.
            return null;
        }

        @Override
        public <A> A getSingleTail(final String label) {
            // Provide a null to bounce the search back to ImmutablePath.getSingleTail.
            return null;
        }

        @Override
        public boolean hasLabel(final String label) {
            return false;
        }

        @Override
        public void addLabel(final String label) {
            throw new UnsupportedOperationException("A head path can not have labels added to it");
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
        public HeadPath clone() {
            return this;
        }

        public static HeadPath instance() {
            return INSTANCE;
        }

        @Override
        public boolean equals(final Object object) {
            return object instanceof HeadPath;
        }

        @Override
        public String toString() {
            return Collections.emptyList().toString();
        }
    }
}
