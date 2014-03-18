package com.tinkerpop.gremlin.process.graph.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Tree<T> extends HashMap<T, Tree<T>> {

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
        Collection<Tree<T>> values = this.values();
        return values.iterator().next().isEmpty();

    }

    public static <T> Map.Entry<T, Tree<T>> createTree(T key, Tree<T> tree) {
        return new SimpleEntry<>(key, tree);
    }
}
