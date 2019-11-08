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
package org.apache.tinkerpop.gremlin.util;

import org.apache.tinkerpop.gremlin.AssertHelper;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TimeUtilTest {

    @Test
    public void shouldBeUtilityClass() throws Exception {
        AssertHelper.assertIsUtilityClass(TimeUtil.class);
    }

    @Test
    public void shouldRunProcess() {
        final AtomicInteger ping = new AtomicInteger(0);
        TimeUtil.clock(ping::incrementAndGet);

        // expect 1 extra for warmup
        assertEquals(101, ping.get());
    }

    @Test
    public void shouldRunProcess1Time() {
        final AtomicInteger ping = new AtomicInteger(0);
        TimeUtil.clock(1, ping::incrementAndGet);

        // expect 1 extra for warmup
        assertEquals(2, ping.get());
    }

}
