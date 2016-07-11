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
import org.apache.tinkerpop.gremlin.process.traversal.Pop;

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

    private ImmutablePathImpl previousPath = TailPath.instance();
    private Object currentObject;
    private Set<String> currentLabels = new LinkedHashSet<>();

    protected ImmutablePath() {

    }

    public static Path make() {
        return TailPath.instance();
    }

    @SuppressWarnings("CloneDoesntCallSuperClone,CloneDoesntDeclareCloneNotSupportedException")
    @Override
    public ImmutablePath clone() {
        return this;
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
    public Path extend(final Set<String> labels) {
        final Set<String> temp = new LinkedHashSet<>();
        temp.addAll(this.currentLabels);
        temp.addAll(labels);
        return new ImmutablePath(this.previousPath, this.currentObject, temp);
    }

    @Override
    public Path retract(final Set<String> labels) {
        if (labels.isEmpty())
            return this;

        // get all the immutable path sections
        final List<ImmutablePath> immutablePaths = new ArrayList<>();
        ImmutablePath currentPathSection = this;
        while (true) {
            immutablePaths.add(0, currentPathSection);
            if (currentPathSection.previousPath instanceof TailPath)
                break;
            else
                currentPathSection = (ImmutablePath) currentPathSection.previousPath;
        }
        // build a new immutable path using the respective path sections that are not to be retracted
        Path newPath = TailPath.instance();
        for (final ImmutablePath immutablePath : immutablePaths) {
            final Set<String> temp = new LinkedHashSet<>(immutablePath.currentLabels);
            temp.removeAll(labels);
            if (!temp.isEmpty())
                newPath = newPath.extend(immutablePath.currentObject, temp);
        }
        return newPath;
    }

    @Override
    public <A> A get(final int index) {
        return (this.size() - 1) == index ? (A) this.currentObject : this.previousPath.get(index);
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
    public <A> A get(final Pop pop, final String label) {
        if (Pop.all == pop) {
            // Recursively build the list to avoid building objects/labels collections.
            final List<A> list = this.previousPath.get(Pop.all, label);
            // Add our object, if our step labels match.
            if (this.currentLabels.contains(label))
                list.add((A) currentObject);
            return (A) list;
        } else {
            // Delegate to the non-throwing, optimized head/tail calculations.
            final A single = Pop.first == pop ? this.getSingleTail(label) : this.getSingleHead(label);
            // Throw if we didn't find the label.
            if (null == single)
                throw Path.Exceptions.stepWithProvidedLabelDoesNotExist(label);
            return single;
        }
    }

    @Override
    public boolean hasLabel(final String label) {
        return this.currentLabels.contains(label) || this.previousPath.hasLabel(label);
    }

    @Override
    public List<Object> objects() {
        final List<Object> objectPath = new ArrayList<>();    // TODO: optimize
        objectPath.addAll(this.previousPath.objects());
        objectPath.add(this.currentObject);
        return Collections.unmodifiableList(objectPath);
    }

    @Override
    public List<Set<String>> labels() {
        final List<Set<String>> labelPath = new ArrayList<>();   // TODO: optimize
        labelPath.addAll(this.previousPath.labels());
        labelPath.add(this.currentLabels);
        return Collections.unmodifiableList(labelPath);
    }

    @Override
    public String toString() {
        return this.objects().toString();
    }

    @Override
    public int hashCode() {
        return this.objects().hashCode();
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof Path))
            return false;
        final Path otherPath = (Path) other;
        if (otherPath.size() != this.size())
            return false;
        for (int i = this.size() - 1; i >= 0; i--) {
            if (!this.get(i).equals(otherPath.get(i)))
                return false;
            if (!this.labels().get(i).equals(otherPath.labels().get(i)))
                return false;
        }
        return true;
    }


    private static class TailPath implements Path, ImmutablePathImpl, Serializable {
        private static final TailPath INSTANCE = new TailPath();

        private TailPath() {

        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public Path extend(final Object object, final Set<String> labels) {
            return new ImmutablePath(TailPath.instance(), object, labels);
        }

        @Override
        public Path extend(final Set<String> labels) {
            if (labels.size() == 0) {
                return this;
            }
            throw new UnsupportedOperationException("A head path can not have labels added to it");
        }

        @Override
        public Path retract(final Set<String> labels) {
            return this;
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
        public <A> A get(final Pop pop, final String label) {
            return pop == Pop.all ? (A) new ArrayList<>() : null;
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

        @SuppressWarnings("CloneDoesntCallSuperClone,CloneDoesntDeclareCloneNotSupportedException")
        @Override
        public TailPath clone() {
            return this;
        }

        public static TailPath instance() {
            return INSTANCE;
        }

        @Override
        public String toString() {
            return Collections.emptyList().toString();
        }

        @Override
        public int hashCode() {
            return Collections.emptyList().hashCode();
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof Path && ((Path) other).size() == 0;
        }
    }
}
