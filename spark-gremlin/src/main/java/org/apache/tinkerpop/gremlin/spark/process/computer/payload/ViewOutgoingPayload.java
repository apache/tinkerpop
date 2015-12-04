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
package org.apache.tinkerpop.gremlin.spark.process.computer.payload;

import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import scala.Tuple2;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ViewOutgoingPayload<M> implements Payload {

    private List<DetachedVertexProperty<Object>> view;
    private List<Tuple2<Object, M>> outgoingMessages;

    private ViewOutgoingPayload() {

    }

    public ViewOutgoingPayload(final List<DetachedVertexProperty<Object>> view, final List<Tuple2<Object, M>> outgoingMessages) {
        this.view = view;
        this.outgoingMessages = outgoingMessages;
    }

    public ViewPayload getView() {
        return new ViewPayload(this.view);
    }

    public List<Tuple2<Object, M>> getOutgoingMessages() {
        return this.outgoingMessages;
    }
}
