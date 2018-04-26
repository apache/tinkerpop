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
package org.apache.tinkerpop.gremlin.process.traversal;

/**
 * Many {@link Step} instance can have a variable scope which alter the manner in which the step will behave in
 * relation to how the traversers are processed.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum Scope {

    /**
     * Informs the step to operate on the entire traversal.
     *
     * @since 3.0.0-incubating
     */
    global,

    /**
     * Informs the step to operate on the current object in the step.
     *
     * @since 3.0.0-incubating
     */
    local;

    public Scope opposite() {
        return global.equals(this) ? local : global;
    }

}
