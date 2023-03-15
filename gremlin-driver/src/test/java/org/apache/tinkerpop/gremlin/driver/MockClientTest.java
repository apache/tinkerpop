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
package org.apache.tinkerpop.gremlin.driver;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This is an example of how to mock the {@link Client} for a script and how to return a specific {@link ResultSet}.
 * The problem with mocking here mostly has to do with having to try to mocking {@link ResultSet} which is marked
 * as {@code final}. Fortunately, mockito can mock such classes if your {@code src/test/resources/mockito-extensions}
 * has a file called {@code org.mockito.plugins.MockMaker} with the single line of {@code mock-maker-inline} in it.
 * Then, mocking works as normal.
 */
public class MockClientTest {

    @Test
    public void shouldBeAbleToMockClient() throws Exception {
        final Client mockClient = mock(Client.class);
        final ResultSet mockResultSet = mock(ResultSet.class);
        final CompletableFuture<List<Result>> results = new CompletableFuture<>();
        results.complete(Arrays.asList(new Result(100), new Result(200), new Result(300)));
        when(mockResultSet.all()).thenReturn(results);
        when(mockClient.submit(anyString())).thenReturn(mockResultSet);

        final CompletableFuture<List<Result>> rs = mockClient.submit("g.V().count()").all();
        assertEquals(100, rs.get().get(0).getInt());
        assertEquals(200, rs.get().get(1).getInt());
        assertEquals(300, rs.get().get(2).getInt());
    }
}
