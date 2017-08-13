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
package org.apache.tinkerpop.gremlin.object.traversal.library;

import org.apache.tinkerpop.gremlin.object.traversal.TraversalTest;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Verify that {@link HasTest} traverses the has step with the provided arguments.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
public class HasTest extends TraversalTest<Element> {

  @Test
  public void testHasTraversal() {
    Has has = Has.of("label", "key", "value");

    traverse(has);

    verify(traversal, times(1)).has("label", "key", "value");
    assertEquals("label", has.getLabel());
    assertEquals("key", has.getKey());
    assertEquals("value", has.getValue());
  }

}
