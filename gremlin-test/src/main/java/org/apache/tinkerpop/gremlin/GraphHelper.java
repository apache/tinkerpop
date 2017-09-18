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
package org.apache.tinkerpop.gremlin;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;

/**
 * Utility class supporting common functions for {@link org.apache.tinkerpop.gremlin.structure.Graph}.
 */
public final class GraphHelper {

  private GraphHelper() {
  }

  /**
   * make a deep clone of the graph elements that preserves ids
   */
  public static void cloneElements(final Graph original, final Graph clone) {
    original.vertices().forEachRemaining(v -> DetachedFactory.detach(v, true).attach(Attachable.Method.create(clone)));
    original.edges().forEachRemaining(e -> DetachedFactory.detach(e, true).attach(Attachable.Method.create(clone)));
  }
}
