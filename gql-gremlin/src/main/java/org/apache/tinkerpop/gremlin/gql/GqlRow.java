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

import java.util.List;
import java.util.Map;

/**
 * An immutable result row produced by a {@link GqlExecutor}. It carries two parallel channels
 * keyed by the variable index defined in the {@link GqlMatchPlan}:
 *
 * <ul>
 *   <li>{@link #scalars}: the flat {@code Element[]} of scalar bindings. Each slot holds a single
 *       graph {@link Element} (or {@code null} when the variable is unbound). Group (list)
 *       variables are carried separately, so this array holds only real graph elements.</li>
 *   <li>{@link #groups}: a side map from variable index to the ordered {@code List<Edge>} bound
 *       to a group edge variable produced by a quantified (variable-length) relationship pattern.
 *       Non-quantified matches carry an empty map.</li>
 * </ul>
 *
 * <p>Keeping group lists out of the scalar array lets the executor keep a homogeneous
 * {@code Element[]} with simple backtracking while still surfacing variable-length walks as
 * selectable {@code List<Edge>} values.</p>
 */
final class GqlRow {

    /** Scalar bindings by variable index; each slot is a single graph {@link Element} or {@code null}. */
    final Element[] scalars;

    /** Group edge-variable bindings by variable index; empty for non-quantified matches. */
    final Map<Integer, List<Edge>> groups;

    GqlRow(final Element[] scalars, final Map<Integer, List<Edge>> groups) {
        this.scalars = scalars;
        this.groups = groups;
    }
}
