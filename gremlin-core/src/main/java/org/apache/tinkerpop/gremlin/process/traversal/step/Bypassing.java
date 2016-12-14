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
package org.apache.tinkerpop.gremlin.process.traversal.step;

/**
 * A {@code ByPassing} step can be stated (at runtime) to simply enact the identity function.
 * This is useful in for steps that need to dynamically change their behavior on {@link org.apache.tinkerpop.gremlin.process.computer.GraphComputer}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @deprecated As of release 3.3.0, replaced by {@link Distributing}
 */
@Deprecated
public interface Bypassing {

    @Deprecated
    public void setBypass(final boolean bypass);
}
