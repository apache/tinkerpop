/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.object.structure;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.util.function.Predicate;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import static org.apache.tinkerpop.gremlin.object.reflect.Classes.is;
import static org.apache.tinkerpop.gremlin.object.reflect.Classes.isVertex;

/**
 * {@link HasFeature} is a predicate that given the {@link org.apache.tinkerpop.gremlin.structure.Graph.Features}
 * of the gremlin graph, determines whether a particular feature is available.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@FunctionalInterface
public interface HasFeature extends Predicate<Graph.Features> {

  /**
   * The {@link Verifier} tests the given {@link HasFeature} against the features obtained from a
   * {@link GraphTraversalSource}. If it contains an {@link EmptyGraph}, then the feature is assumed
   * to be unavailable.
   */
  @Data
  @RequiredArgsConstructor(staticName = "of")
  class Verifier {

    private final GraphTraversalSource g;

    public boolean verify(HasFeature hasFeature) {
      return g.getGraph() != null && !is(g.getGraph(), EmptyGraph.class) &&
          hasFeature.test(g.getGraph().features());
    }
  }

  static HasFeature supportsUserSuppliedIds(Element element) {
    return features -> (isVertex(element) ? features.vertex() : features.edge())
        .supportsUserSuppliedIds();
  }

  static HasFeature supportsGraphAdd(Element element) {
    return features -> isVertex(element) ? features.vertex().supportsAddVertices()
        : features.edge().supportsAddEdges();
  }

  static HasFeature supportsGraphAddVertex() {
    return features -> features.vertex().supportsAddVertices();
  }

  static HasFeature supportsGraphAddEdge() {
    return features -> features.edge().supportsAddEdges();
  }
}
