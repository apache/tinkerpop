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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinTest {
    @Test
    public void shouldBeUtilityClass() throws Exception {
        AssertHelper.assertIsUtilityClass(Gremlin.class);
    }

    @Test
    public void shouldGetVersion() {
        String versionString = Gremlin.version();
        //Remove suffixes such as -SNAPSHOT from version for test
        if (versionString.contains("-")) {
            versionString = versionString.substring(0, versionString.indexOf("-"));
        }
        String[] version = versionString.replace("-SNAPSHOT", "").split("\\.");

        assertEquals("Gremlin.version() should be in format of x.y.z", 3, version.length);
        assertTrue("Major version should be greater than 3", Integer.parseInt(version[0]) > 3);
        assertTrue("Minor version should be a positive int", Integer.parseInt(version[1]) >= 0);
        assertTrue("Patch version should be a positive int", Integer.parseInt(version[2]) >= 0);
    }

    @Test
    public void shouldGetMajorVersion() {
        String majorVersion = Gremlin.majorVersion();

        assertTrue("Major version should be greater than 3", Integer.parseInt(majorVersion) > 3);
    }
}
