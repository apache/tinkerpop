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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * {@link Observable} manages the observers that implement the interface defined by the {@link O}
 * type parameter.
 *
 * <p>
 * The {@link Graph} is an example of an {@link Observable}, in that updates to it can be observed
 * through its {@code Observer} interface.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
public class Observable<O> {

  private final List<O> observers = Collections.synchronizedList(new ArrayList<>());

  public void register(O observer) {
    observers.add(observer);
  }

  public void unregister(O observer) {
    observers.remove(observer);
  }

  public void notifyAll(Consumer<O> change) {
    observers.forEach(change);
  }
}
