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
package org.apache.tinkerpop.gremlin.hadoop.process.computer.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MessageBox<M> implements Serializable {

    protected final List<M> incoming;
    protected final Map<Object, List<M>> outgoing = new HashMap<>();

    public MessageBox() {
        this(new ArrayList<>());
    }

    public MessageBox(final List<M> incomingMessages) {
        this.incoming = incomingMessages;
    }

    public void sendMessage(final Object vertexId, final M message) {
        List<M> messages = this.outgoing.get(vertexId);
        if (null == messages) {
            messages = new ArrayList<>();
            this.outgoing.put(vertexId, messages);
        }
        messages.add(message);
    }

    public List<M> receiveMessages() {
        return this.incoming;
    }

    public void clearIncomingMessages() {
        this.incoming.clear();
    }

    @Override
    public String toString() {
        return "messageBox[incoming(" + this.incoming.size() + "):outgoing(" + this.outgoing.size() + ")]";
    }
}
