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
package org.apache.tinkerpop.gremlin.object.reflect;

import org.apache.tinkerpop.gremlin.object.edges.Develops;
import org.apache.tinkerpop.gremlin.object.vertices.Location;
import org.apache.tinkerpop.gremlin.object.vertices.Person;
import org.junit.Before;

import java.lang.reflect.Field;

import static org.apache.tinkerpop.gremlin.object.reflect.Fields.field;

/**
 * This acts as a base of all tests in the reflect package.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
public abstract class ReflectTest {

  protected Person marko;
  protected Develops develops;
  protected Location location;

  protected Field name;
  protected Field locations;

  @Before
  public void setUp() {
    marko = Person.of("marko", 29);
    develops = Develops.of(2000);
    location = Location.of("san diego", 2000);
    name = field(marko, "name");
    locations = field(marko, "locations");
  }
}
