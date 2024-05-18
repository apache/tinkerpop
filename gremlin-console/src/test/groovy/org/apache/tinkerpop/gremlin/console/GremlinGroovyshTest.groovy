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
package org.apache.tinkerpop.gremlin.console


import org.apache.tinkerpop.gremlin.console.jsr223.AbstractGremlinServerIntegrationTest
import org.codehaus.groovy.tools.shell.IO

class GremlinGroovyshTest extends AbstractGremlinServerIntegrationTest {
    private IO testio
    private ByteArrayOutputStream out
    private ByteArrayOutputStream err
    private GremlinGroovysh shell

    @Override
    void setUp() {
        super.setUp()
        out = new ByteArrayOutputStream()
        err = new ByteArrayOutputStream()
        testio = new IO(new ByteArrayInputStream(), out, err)
        shell = new GremlinGroovysh(new Mediator(null), testio)
    }

    @Override
    void tearDown() {
        super.tearDown()
        shell.execute(":purge preferences") // for test cases where persistent preferences (interpreterMode) are set.
    }
}
