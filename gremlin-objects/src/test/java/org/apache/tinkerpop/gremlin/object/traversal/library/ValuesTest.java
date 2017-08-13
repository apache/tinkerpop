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
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verify that {@link Values} gets the values of all the properties of the given element, de-duped.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
public class ValuesTest extends TraversalTest<Object> {

  @Test
  public void testValuesTraversal() {
    City sanFrancisco = City.of("San Francisco");
    Values values = Values.of(sanFrancisco);

    when(traversal.values(anyVararg())).thenReturn(traversal);
    traverse(values);

    verify(traversal, times(1)).values("name", "population");
    verify(traversal, times(1)).dedup();
    assertArrayEquals(new String[] {"name", "population"}, values.getPropertyKeys());
  }

}
