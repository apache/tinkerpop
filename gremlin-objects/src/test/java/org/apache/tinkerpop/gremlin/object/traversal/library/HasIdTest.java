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
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Verify that {@link HasId} checks if the traversal has the generated id, since its {@link City#id}
 * is missing.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@SuppressWarnings("serial")
public class HasIdTest extends TraversalTest<Element> {

  @Test
  public void testHasIdTraversal() {
    HasId hasId = HasId.of(sanFrancisco);

    traverse(hasId);

    verify(traversal, times(1)).hasId(new HashMap<String, Object>() {
      {
        put(T.label.getAccessor(), sanFrancisco.label());
        put("name", "San Francisco");
      }
    });
    assertEquals(sanFrancisco, hasId.getElement());
  }

}
