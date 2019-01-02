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

import java.io.Serializable;
import java.util.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Tree<T> extends HashMap<T, Tree<T>> implements Serializable {

    public Tree() {
        super();
    }

    @SafeVarargs
    public Tree(final T... children) {
        this();
        for (final T t : children) {
            this.put(t, new Tree<>());
        }
    }

    @SafeVarargs
    public Tree(final Map.Entry<T, Tree<T>>... children) {
        this();
        for (final Map.Entry<T, Tree<T>> entry : children) {
            this.put(entry.getKey(), entry.getValue());
        }
    }


    public List<Tree<T>> getTreesAtDepth(final int depth) {
        List<Tree<T>> currentDepth = Collections.singletonList(this);
        for (int i = 0; i < depth; i++) {
            if (i == depth - 1) {
                return currentDepth;
            } else {
                final List<Tree<T>> temp = new ArrayList<>();
                for (final Tree<T> t : currentDepth) {
                    temp.addAll(t.values());
                }
                currentDepth = temp;
            }
        }
        return Collections.emptyList();
    }

    public List<T> getObjectsAtDepth(final int depth) {
        final List<T> list = new ArrayList<>();
        for (final Tree<T> t : this.getTreesAtDepth(depth)) {
            list.addAll(t.keySet());
        }
        return list;
    }

    public List<Tree<T>> getLeafTrees() {
        final List<Tree<T>> leaves = new ArrayList<>();
        List<Tree<T>> currentDepth = Collections.singletonList(this);
        boolean allLeaves = false;
        while (!allLeaves) {
            allLeaves = true;
            final List<Tree<T>> temp = new ArrayList<>();
            for (final Tree<T> t : currentDepth) {
                if (t.isLeaf()) {
                    for (Map.Entry<T, Tree<T>> t2 : t.entrySet()) {
                        leaves.add(new Tree<>(t2));
                    }
                } else {
                    allLeaves = false;
                    temp.addAll(t.values());
                }
            }
            currentDepth = temp;

        }
        return leaves;
    }

    public List<T> getLeafObjects() {
        final List<T> leaves = new ArrayList<>();
        for (final Tree<T> t : this.getLeafTrees()) {
            leaves.addAll(t.keySet());
        }
        return leaves;
    }

    public boolean isLeaf() {
        final Collection<Tree<T>> values = this.values();
        return values.iterator().next().isEmpty();

    }

    public void addTree(final Tree<T> tree) {
        tree.forEach((k, t) -> {
            if (this.containsKey(k)) {
                this.get(k).addTree(t);
            } else {
                this.put(k, t);
            }
        });
    }

    public List<Tree<T>> splitParents() {
        if (this.keySet().size() == 1) {
            return Collections.singletonList(this);
        } else {
            final List<Tree<T>> parents = new ArrayList<>();
            this.forEach((k, t) -> {
                final Tree<T> parentTree = new Tree<>();
                parentTree.put(k, t);
                parents.add(parentTree);
            });
            return parents;
        }
    }
}
