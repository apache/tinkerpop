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
import org.apache.tinkerpop.gremlin.object.vertices.Person;
import org.junit.Test;

/**
 * Verify that the helper methods in the {@link Vertex} class work as expected.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
public class VertexTest extends ElementTest<Person> {

  @Override
  protected Person createElement() {
    return Person.of("marko", 29, Location.of("san diego", 1997, 2001),
        Location.of("santa cruz", 2001, 2004),
        Location.of("brussels", 2004, 2005),
        Location.of("santa fe", 2005));
  }

  @Override
  protected Person anotherElement() {
    return Person.of("daniel",
        Location.of("spremberg", 1982, 2005),
        Location.of("kaiserslautern", 2005, 2009),
        Location.of("aachen", 2009));
  }

  @Test(expected = IllegalStateException.class)
  public void testCannotDeleteDetachedVertex() {
    Person person = createElement();
    person.remove();
  }
}
