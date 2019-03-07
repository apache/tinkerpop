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
package org.apache.tinkerpop.machine.compiler;

import org.apache.tinkerpop.language.Traversal;
import org.apache.tinkerpop.machine.processor.EmptyProcessorFactory;
import org.apache.tinkerpop.machine.bytecode.BytecodeUtil;
import org.junit.jupiter.api.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BytecodeUtilTest {

    @Test
    public void shouldHaveBytecode() throws Exception {
        final Traversal<Long, Long, Long> traversal = new Traversal<>(1L, EmptyProcessorFactory.instance());
        traversal.incr().is(2L);
        //System.out.println(traversal.bytecode);
        //System.out.println(BytecodeUtil.compile(traversal.getBytecode()));
    }
}
