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
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ImmutablePath implements Path, Serializable, Cloneable {

    private static final Object END = new Object();
    private static final ImmutablePath TAIL_PATH = new ImmutablePath(null, END, null);

    private ImmutablePath previousPath;
    private Object currentObject;
    private Set<String> currentLabels;

    public static Path make() {
        return TAIL_PATH;
    }

    @SuppressWarnings("CloneDoesntCallSuperClone,CloneDoesntDeclareCloneNotSupportedException")
    @Override
    public ImmutablePath clone() {
        return this;
    }

    private ImmutablePath(final ImmutablePath previousPath, final Object currentObject, final Set<String> currentLabels) {
        this.previousPath = previousPath;
        this.currentObject = currentObject;
        this.currentLabels = currentLabels;
    }

    private final boolean isTail() {
        return END == this.currentObject;
    }

    @Override
    public boolean isEmpty() {
        return this.isTail();
    }

    @Override
    public int size() {
        int counter = 0;
        ImmutablePath currentPath = this;
        while (true) {
            if (currentPath.isTail()) return counter;
            counter++;
            currentPath = currentPath.previousPath;
        }
    }

    @Override
    public <A> A head() {
        return (A) this.currentObject;
    }

    @Override
    public Path extend(final Object object, final Set<String> labels) {
        return new ImmutablePath(this, object, labels);
    }

    @Override
    public Path extend(final Set<String> labels) {
        if (labels.isEmpty() || this.currentLabels.containsAll(labels))
            return this;
        else {
            final Set<String> newLabels = new LinkedHashSet<>();
            newLabels.addAll(this.currentLabels);
            newLabels.addAll(labels);
            return new ImmutablePath(this.previousPath, this.currentObject, newLabels);
        }
    }

    @Override
    public Path retract(final Set<String> labels) {
        if (labels.isEmpty())
            return this;

        // get all the immutable path sections
        final List<ImmutablePath> immutablePaths = new ArrayList<>();
        ImmutablePath currentPath = this;
        while (true) {
            if (currentPath.isTail())
                break;
            immutablePaths.add(0, currentPath);
            currentPath = currentPath.previousPath;
        }
        // build a new immutable path using the respective path sections that are not to be retracted
        Path newPath = TAIL_PATH;
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
        int counter = this.size();
        ImmutablePath currentPath = this;
        while (true) {
            if (index == --counter)
                return (A) currentPath.currentObject;
            currentPath = currentPath.previousPath;
        }
    }

    @Override
    public <A> A get(final Pop pop, final String label) {
        if (Pop.mixed == pop) {
            return this.get(label);
        } else if (Pop.all == pop) {
            // Recursively build the list to avoid building objects/labels collections.
            final List<Object> list = new ArrayList<>();
            ImmutablePath currentPath = this;
            while (true) {
                if (currentPath.isTail())
                    break;
                else if (currentPath.currentLabels.contains(label))
                    list.add(0, currentPath.currentObject);
                currentPath = currentPath.previousPath;
            }
            return (A) list;
        } else if (Pop.last == pop) {
            ImmutablePath currentPath = this;
            while (true) {
                if (currentPath.isTail())
                    throw Path.Exceptions.stepWithProvidedLabelDoesNotExist(label);
                else if (currentPath.currentLabels.contains(label))
                    return (A) currentPath.currentObject;
                else
                    currentPath = currentPath.previousPath;
            }
        } else { // Pop.first
            A found = null;
            ImmutablePath currentPath = this;
            while (true) {
                if (currentPath.isTail())
                    break;
                else if (currentPath.currentLabels.contains(label))
                    found = (A) currentPath.currentObject;
                currentPath = currentPath.previousPath;
            }
            if (null == found)
                throw Path.Exceptions.stepWithProvidedLabelDoesNotExist(label);
            return found;
        }
    }

    @Override
    public boolean hasLabel(final String label) {
        ImmutablePath currentPath = this;
        while (true) {
            if (currentPath.isTail())
                return false;
            else if (currentPath.currentLabels.contains(label))
                return true;
            else
                currentPath = currentPath.previousPath;
        }
    }

    @Override
    public List<Object> objects() {
        final List<Object> objects = new ArrayList<>();
        ImmutablePath currentPath = this;
        while (true) {
            if (currentPath.isTail())
                break;
            objects.add(0, currentPath.currentObject);
            currentPath = currentPath.previousPath;
        }
        return Collections.unmodifiableList(objects);
    }

    @Override
    public List<Set<String>> labels() {
        final List<Set<String>> labels = new ArrayList<>();
        ImmutablePath currentPath = this;
        while (true) {
            if (currentPath.isTail())
                break;
            labels.add(0, currentPath.currentLabels);
            currentPath = currentPath.previousPath;
        }
        return Collections.unmodifiableList(labels);
    }

    @Override
    public String toString() {
        return StringFactory.pathString(this);
    }

    @Override
    public int hashCode() {
        // hashCode algorithm from AbstractList
        int[] hashCodes = new int[this.size()];
        int index = hashCodes.length - 1;
        ImmutablePath currentPath = this;
        while (true) {
            if (currentPath.isTail())
                break;
            hashCodes[index] = Objects.hashCode(currentPath.currentObject);
            currentPath = currentPath.previousPath;
            index--;
        }
        int hashCode = 1;
        for (final int hash : hashCodes) {
            hashCode = hashCode * 31 + hash;
        }
        return hashCode;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof Path))
            return false;
        final Path otherPath = (Path) other;
        int size = this.size();
        if (otherPath.size() != size)
            return false;
        if (size > 0) {
            ImmutablePath currentPath = this;
            final List<Object> otherObjects = otherPath.objects();
            final List<Set<String>> otherLabels = otherPath.labels();
            for (int i = otherLabels.size() - 1; i >= 0; i--) {
                if (currentPath.isTail())
                    return true;
                else if (!currentPath.currentObject.equals(otherObjects.get(i)) ||
                        !currentPath.currentLabels.equals(otherLabels.get(i)))
                    return false;
                else
                    currentPath = currentPath.previousPath;
            }
        }
        return true;
    }

    @Override
    public boolean popEquals(final Pop pop, final Object other) {
        if (!(other instanceof Path))
            return false;
        final Path otherPath = (Path) other;
        ImmutablePath currentPath = this;
        while (true) {
            if (currentPath.isTail())
                break;
            for (final String label : currentPath.currentLabels) {
                if (!otherPath.hasLabel(label) || !this.get(pop, label).equals(otherPath.get(pop, label)))
                    return false;
            }
            currentPath = currentPath.previousPath;
        }
        return true;
    }

    @Override
    public boolean isSimple() {
        final Set<Object> objects = new HashSet<>();
        ImmutablePath currentPath = this;
        while (true) {
            if (currentPath.isTail())
                return true;
            else if (objects.contains(currentPath.currentObject))
                return false;
            else {
                objects.add(currentPath.currentObject);
                currentPath = currentPath.previousPath;
            }
        }
    }
}
