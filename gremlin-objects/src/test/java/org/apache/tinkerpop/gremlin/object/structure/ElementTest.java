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

import org.apache.tinkerpop.gremlin.object.vertices.Location;
import org.apache.tinkerpop.gremlin.object.reflect.Label;
import org.apache.tinkerpop.gremlin.object.traversal.ElementTo;
import org.apache.tinkerpop.gremlin.object.traversal.TraversalTest;
import org.apache.tinkerpop.gremlin.object.traversal.library.Count;
import org.apache.tinkerpop.gremlin.object.traversal.library.HasLabel;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.tinkerpop.gremlin.object.structure.Element.compose;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Verify that the sub-traversals defined in the {@link Element} class works as expected. In
 * addition, it also checks the {@link #equals} method.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
public class ElementTest<O extends Element> extends TraversalTest<O> {

  @SuppressWarnings("unchecked")
  protected O createElement() {
    return (O) Location.of("San Francisco", 2000);
  }

  @SuppressWarnings("unchecked")
  protected O anotherElement() {
    return (O) Location.of("New York", 2000);
  }

  @Test
  public void testWithIdTraversal() {
    Element element = createElement();

    traverse(element.withId);

    verify(traversal, times(1)).hasId(element.id());
  }

  @Test
  public void testWithLabelTraversal() {
    Element element = createElement();

    traverse(element.withLabel);

    verify(traversal, times(1)).hasLabel(element.label());
  }

  @Test
  public void testComposeTraversals() {
    Element element = createElement();
    ElementTo<Object> counter = compose(
        HasLabel.of(element),
        Count.of());

    traverse(counter);

    verify(traversal, times(1)).hasLabel(element.label());
    verify(traversal, times(1)).count();
  }

  @Test
  public void testLabelExists() {
    Element element = createElement();

    assertEquals(Label.of(element), element.label());
  }

  @Test
  public void testExistsIn() {
    Element element = createElement();
    Element other = anotherElement();

    assertTrue(element.existsIn(Arrays.asList(element)));
    assertFalse(element.existsIn(Arrays.asList(other)));
  }

  @Test
  public void testPossessionApostrophe() {
    Element element = createElement();
    List<Long> userSuppliedId = Arrays.asList(1L, 2L, 3L);
    element.setUserSuppliedId(userSuppliedId);
    assertEquals(userSuppliedId, element.id());

    traverse(element.s("userSuppliedId"));

    verify(traversal, times(1)).has(element.label(), "id", element.id());
  }

  @Test
  public void testEqualsByEverything() {
    Element original = createElement();
    Element duplicate = createElement();

    assertEquals(duplicate, original);

    original.setUserSuppliedId("the original identity");
    duplicate.setUserSuppliedId("a duplicate identity");
    assertNotEquals(duplicate, original);
  }

}
