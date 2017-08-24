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

import java.util.Arrays;
import java.util.List;

import lombok.Data;
import lombok.RequiredArgsConstructor;

/**
 * {@link Connection} specifies the type of vertices that an edge goes out from and in to.
 *
 * <p>
 * If one tries to create an edge for an unknown connection, an object exception will be thrown.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Data
@RequiredArgsConstructor(staticName = "of")
public class Connection {

  private final Class<? extends Vertex> fromVertex;
  private final Class<? extends Vertex> toVertex;

  public static List<Connection> list(Class<? extends Vertex> fromVertexClass,
      Class<? extends Vertex> toVertexClass) {
    return list(of(fromVertexClass, toVertexClass));
  }

  public static List<Connection> list(Connection... connections) {
    return Arrays.asList(connections);
  }
}
