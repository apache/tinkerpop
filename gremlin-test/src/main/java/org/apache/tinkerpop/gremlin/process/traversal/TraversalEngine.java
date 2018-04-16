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

import org.apache.tinkerpop.gremlin.process.computer.Computer;

/**
 * A TraversalEngine is a test component that helps determine the engine on which a traversal test will execute. This
 * interface was originally in {@code gremlin-core} but deprecated in favor of {@link Computer}. Since this interface
 * was heavily bound to the test suite it was maintained until 3.4.0 when it was finally moved here to
 * {@code gremlin-test} where it simply serves as testing infrastructure to provide hints on whether a test will be
 * executed with a {@link Computer} or not.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraversalEngine {
    public enum Type {STANDARD, COMPUTER}
}
