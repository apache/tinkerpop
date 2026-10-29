/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.gql;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * One complete match produced by a {@link GqlExecutor}. A row is an immutable value: once built
 * it cannot be changed, and it never shares mutable state with the arrays, maps or lists it was
 * built from.
 *
 * <p>Every binding in a row is addressed by <em>variable index</em>, the position of the
 * variable in {@link GqlMatchPlan#getVariables()}. The seed variable is at index
 * {@link GqlMatchPlan#getSeedVariableIndex()}, and {@link #size()} equals
 * {@link GqlMatchPlan#getVariableCount()} for the plan that produced the row. Variables whose
 * names start with {@link GqlMatchPlan#ANON_VAR_PREFIX} are anonymous pattern elements. They
 * occupy an index but are not surfaced to the traversal.</p>
 *
 * <p>Each index holds at most one kind of binding:</p>
 * <ul>
 *   <li><b>Scalar</b>: a single graph {@link Element}, read with {@link #getScalar(int)}. Node
 *       variables and non-quantified edge variables are scalars.</li>
 *   <li><b>Group</b>: an ordered {@code List<Edge>}, read with {@link #getGroup(int)}. The edge
 *       variable of a quantified (variable-length) relationship such as
 *       {@code (a)-[r:knows]->{1,3}(b)} is a group. The list holds the edges of the walk in
 *       traversal order, first hop first, and is never empty for a matched walk. A group edge
 *       variable is a list even when the walk has a single edge.</li>
 * </ul>
 *
 * <p>An index with neither binding is unbound: {@link #getScalar(int)} and
 * {@link #getGroup(int)} both return {@code null}, and the variable is omitted from the
 * traversal result.</p>
 *
 * <p>Rows have value semantics: two rows are {@link #equals(Object) equal} when they have the
 * same size and the same scalar and group bindings at every index.</p>
 *
 * <p>Executors build rows with {@link #of(Element[], Map)}.</p>
 */
public final class GqlRow {

    private final Element[] scalars;
    private final Map<Integer, List<Edge>> groups;

    private GqlRow(final Element[] scalars, final Map<Integer, List<Edge>> groups) {
        this.scalars = scalars;
        this.groups = groups;
    }

    /**
     * Creates a row from scalar and group bindings, both keyed by variable index.
     *
     * <p>The {@code scalars} array is copied, and the {@code groups} map and each of its lists are
     * copied into unmodifiable collections, so later changes to the arguments do not affect the
     * row. For an index that is a group, the matching {@code scalars} slot must be {@code null}.</p>
     *
     * @param scalars scalar bindings; {@code scalars[i]} is the {@link Element} bound to variable
     *                index {@code i}, or {@code null}. Its length is the row {@link #size()}.
     * @param groups  group edge-variable bindings from variable index to the ordered edges of the
     *                walk; use an empty map when the row has no group variables
     * @return the new row
     * @throws NullPointerException     if {@code scalars}, {@code groups}, a key of
     *                                  {@code groups}, a group list, or an edge in a group list is
     *                                  {@code null}
     * @throws IllegalArgumentException if a group index is outside {@code [0, scalars.length)},
     *                                  the same index also has a non-null scalar binding, or a
     *                                  group list is empty
     */
    public static GqlRow of(final Element[] scalars, final Map<Integer, List<Edge>> groups) {
        Objects.requireNonNull(scalars, "scalars");
        Objects.requireNonNull(groups, "groups");
        final Element[] scalarsCopy = scalars.clone();
        if (groups.isEmpty())
            return new GqlRow(scalarsCopy, Collections.emptyMap());

        final Map<Integer, List<Edge>> groupsCopy = new HashMap<>(groups.size());
        for (final Map.Entry<Integer, List<Edge>> e : groups.entrySet()) {
            final Integer index = Objects.requireNonNull(e.getKey(), "group index");
            if (index < 0 || index >= scalarsCopy.length)
                throw new IllegalArgumentException("Group index " + index +
                        " is outside the row of size " + scalarsCopy.length);
            if (scalarsCopy[index] != null)
                throw new IllegalArgumentException("Variable index " + index +
                        " cannot be bound as both a scalar and a group");
            final List<Edge> group = Objects.requireNonNull(e.getValue(), "group list");
            if (group.isEmpty())
                throw new IllegalArgumentException("Group list for variable index " + index +
                        " is empty; a matched walk has at least one edge");
            groupsCopy.put(index, List.copyOf(group));
        }
        return new GqlRow(scalarsCopy, Collections.unmodifiableMap(groupsCopy));
    }

    /**
     * Returns the number of variable indices in this row, equal to the length of the scalar array
     * the row was built from.
     */
    public int size() {
        return scalars.length;
    }

    /**
     * Returns the scalar {@link Element} bound to the variable index, or {@code null} if the index
     * is unbound or holds a group.
     *
     * @throws IndexOutOfBoundsException if {@code index} is outside {@code [0, size())}
     */
    public Element getScalar(final int index) {
        Objects.checkIndex(index, scalars.length);
        return scalars[index];
    }

    /**
     * Returns the unmodifiable, ordered edge list bound to the variable index, or {@code null} if
     * the index is unbound or holds a scalar.
     *
     * @throws IndexOutOfBoundsException if {@code index} is outside {@code [0, size())}
     */
    public List<Edge> getGroup(final int index) {
        Objects.checkIndex(index, scalars.length);
        return groups.get(index);
    }

    /**
     * Returns {@code true} if the variable index holds a group (edge list) binding.
     *
     * @throws IndexOutOfBoundsException if {@code index} is outside {@code [0, size())}
     */
    public boolean isGroup(final int index) {
        Objects.checkIndex(index, scalars.length);
        return groups.containsKey(index);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (!(o instanceof GqlRow))
            return false;
        final GqlRow other = (GqlRow) o;
        return Arrays.equals(scalars, other.scalars) && groups.equals(other.groups);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(scalars) + groups.hashCode();
    }

    @Override
    public String toString() {
        return "GqlRow{scalars=" + Arrays.toString(scalars) + ", groups=" + groups + "}";
    }
}
