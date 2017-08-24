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
 * Verify that {@link HasKeys} steps through the given element's label, followed by it's
 * primary key property(s).
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
public class AddVTest extends TraversalTest<Element> {

  @Test
  public void testAddKeyTraversal() {
    AddV addV = AddV.of(sanFrancisco);

    traverse(addV);

    verify(g, times(1)).addV(sanFrancisco.label());
    verify(traversal, times(1)).property("name", "San Francisco");
    assertEquals(sanFrancisco, addV.getElement());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddNullKeyTraversal() {
    City nowhere = City.of(null);
    AddV addV = AddV.of(nowhere);

    traverse(addV);
  }
}
