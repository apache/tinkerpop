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

import org.apache.tinkerpop.gremlin.object.edges.Develops;
import org.apache.tinkerpop.gremlin.object.vertices.Person;
import org.apache.tinkerpop.gremlin.object.vertices.Software;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Assert that the {@link Edge#connects} methods work.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
public class EdgeTest extends ElementTest<Develops> {

  @Override
  protected Develops createElement() {
    return Develops.of(2010);
  }

  @Override
  protected Develops anotherElement() {
    return Develops.of(2017);
  }

  @Test
  public void testConnectsProperly() {
    Develops develops = createElement();

    assertTrue(develops.connects(Person.class, Software.class));
    assertFalse(develops.connects(Person.class, Person.class));
  }
}
