/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tinkerpop.example.cmdb.unit;

import org.apache.tinkerpop.example.cmdb.graph.GremlinGateway;
import org.apache.tinkerpop.example.cmdb.service.ServiceGraphService;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ServiceGraphServiceTest {

    @Test
    void dependenciesDelegatesToGateway() {
        GremlinGateway gateway = mock(GremlinGateway.class);
        List<Map<Object, Object>> expected = List.of(Map.of("k", "v"));
        when(gateway.fetch(any())).thenReturn((List) expected);

        ServiceGraphService svc = new ServiceGraphService(gateway);
        List<Map<Object, Object>> result = svc.dependencies("svc-1", 2);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void ownersDelegatesToGateway() {
        GremlinGateway gateway = mock(GremlinGateway.class);
        List<Map<Object, Object>> expected = List.of(Map.of("team", "platform"));
        when(gateway.fetch(any())).thenReturn((List) expected);

        ServiceGraphService svc = new ServiceGraphService(gateway);
        List<Map<Object, Object>> result = svc.owners("svc-1");
        assertThat(result).isEqualTo(expected);
    }
}
