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
package org.apache.tinkerpop.gremlin.process.computer;

import java.io.Serializable;

/**
 * A MessageCombiner allows two messages in route to the same vertex to be aggregated into a single message.
 * Message combining can reduce the number of messages sent between vertices and thus, reduce network traffic.
 * Not all messages can be combined and thus, this is an optional feature of a {@link VertexProgram}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface MessageCombiner<M> extends Serializable {

    /**
     * Combine two messages and return a message containing the combination.
     * In many instances, it is possible to simply merge the data in the second message into the first message.
     * Such an optimization can limit the amount of object creation.
     *
     * @param messageA the first message
     * @param messageB the second message
     * @return the combination of the two messages
     */
    public M combine(final M messageA, final M messageB);
}
