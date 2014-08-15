package com.tinkerpop.gremlin.process.graph.step.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        final List<Tree<T>> branches = new ArrayList<Tree<T>>();
        List<Tree<T>> currentDepth = Arrays.asList(this);
        for (int i = 0; i < depth; i++) {
            if (i == depth - 1) {
                return currentDepth;
            } else {
                final List<Tree<T>> temp = new ArrayList<Tree<T>>();
                for (final Tree<T> t : currentDepth) {
                    temp.addAll(t.values());
                }
                currentDepth = temp;
            }
        }
        return branches;
    }

    public List<T> getObjectsAtDepth(final int depth) {
        final List<T> list = new ArrayList<T>();
        for (final Tree<T> t : this.getTreesAtDepth(depth)) {
            list.addAll(t.keySet());
        }
        return list;
    }

    public List<Tree<T>> getLeafTrees() {
        final List<Tree<T>> leaves = new ArrayList<>();
        List<Tree<T>> currentDepth = Arrays.asList(this);
        boolean allLeaves = false;
        while (!allLeaves) {
            allLeaves = true;
            final List<Tree<T>> temp = new ArrayList<>();
            for (final Tree<T> t : currentDepth) {
                if (t.isLeaf()) {
                    for (Map.Entry<T, Tree<T>> t2 : t.entrySet()) {
                        leaves.add(new Tree<T>(t2));
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
        final List<T> leaves = new ArrayList<T>();
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
            return Arrays.asList(this);
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
