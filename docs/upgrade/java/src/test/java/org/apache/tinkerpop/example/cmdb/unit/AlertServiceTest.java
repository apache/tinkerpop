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

import org.apache.tinkerpop.example.cmdb.errors.ApiException;
import org.apache.tinkerpop.example.cmdb.graph.GremlinGateway;
import org.apache.tinkerpop.example.cmdb.service.AlertService;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class AlertServiceTest {

    @Test
    void missingAlertIdThrows() {
        GremlinGateway gateway = mock(GremlinGateway.class);
        AlertService svc = new AlertService(gateway);

        Map<String, Object> payload = new HashMap<>();
        assertThatThrownBy(() -> svc.upsertAlert(payload))
                .isInstanceOf(ApiException.class)
                .hasMessageContaining("alertId is required");

        verifyNoInteractions(gateway);
    }

    @Test
    void validPayloadCallsIterate() {
        GremlinGateway gateway = mock(GremlinGateway.class);
        AlertService svc = new AlertService(gateway);

        Map<String, Object> payload = new HashMap<>();
        payload.put("alertId", "a-1");
        payload.put("source", "prom");
        payload.put("metric", "cpu");
        payload.put("firedAt", 1700000000000L);

        svc.upsertAlert(payload);

        verify(gateway, times(1)).iterate(any());
        verifyNoMoreInteractions(gateway);
    }
}
