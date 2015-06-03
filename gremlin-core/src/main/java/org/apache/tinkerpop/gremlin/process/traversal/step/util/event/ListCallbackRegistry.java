/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.step.util.event;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Matt Frantz (http://github.com/mhfrantz)
 */
public class ListCallbackRegistry<E extends Event> implements CallbackRegistry<E> {
    private List<EventCallback<E>> callbacks;

    @Override
    public void addCallback(final EventCallback<E> c) {
        if (callbacks == null) callbacks = new ArrayList<>();
        callbacks.add(c);
    }

    @Override
    public void removeCallback(final EventCallback<E> c) {
        if (callbacks != null) callbacks.remove(c);
    }

    @Override
    public void clearCallbacks() {
        if (callbacks != null) callbacks.clear();
    }

    @Override
    public List<EventCallback<E>> getCallbacks() {
        return (callbacks != null) ? Collections.unmodifiableList(callbacks) : Collections.emptyList();
    }
}
