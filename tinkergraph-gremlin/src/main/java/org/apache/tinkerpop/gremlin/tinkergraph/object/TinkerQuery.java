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
package org.apache.tinkerpop.gremlin.tinkergraph.object;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.object.provider.GraphSystem;
import org.apache.tinkerpop.gremlin.object.traversal.ObjectQuery;

import javax.inject.Inject;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

/**
 * The {@link TinkerQuery} is the default implementation of the {@link ObjectQuery}, which lets you
 * {@link org.apache.tinkerpop.gremlin.object.traversal.Query} objects created using the {@link
 * org.apache.tinkerpop.gremlin.object.structure.Graph}. If dependency injection is not enabled, use
 * the {@link TinkerFactory}.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Data
@Slf4j
@EqualsAndHashCode(callSuper = true)
public class TinkerQuery extends ObjectQuery {

  @Inject
  public TinkerQuery(GraphSystem<Configuration> system) {
    super(system);
  }
}
