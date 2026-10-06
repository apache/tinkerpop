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
package org.apache.tinkerpop.gremlin.structure.snapshot.graph;

import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * An {@link Element} facade of a {@link CsrGraph}, which is a vertex or edge ordinal in the graph's snapshot. Native
 * execution uses it to turn the elements of upstream traversers back into ordinals.
 */
public interface CsrElement extends Element {

    /**
     * The graph whose snapshot the ordinal belongs to.
     */
    @Override
    CsrGraph graph();

    /**
     * The vertex ordinal of a {@link CsrVertex} or the edge ordinal of a {@link CsrEdge}.
     */
    int ordinal();
}
