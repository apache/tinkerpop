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
package org.apache.tinkerpop.gremlin.process.computer.util;

import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SingleMessenger<M> implements Messenger<M> {

    private final Messenger<M> baseMessenger;
    private final M message;

    public SingleMessenger(final Messenger<M> baseMessenger, final M message) {
        this.baseMessenger = baseMessenger;
        this.message = message;
    }

    @Override
    public Iterator<M> receiveMessages() {
        return IteratorUtils.of(this.message);
    }

    @Override
    public void sendMessage(final MessageScope messageScope, final M message) {
        this.baseMessenger.sendMessage(messageScope, message);
    }
}
