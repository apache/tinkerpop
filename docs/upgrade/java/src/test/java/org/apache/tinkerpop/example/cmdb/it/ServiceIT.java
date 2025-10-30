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
package org.apache.tinkerpop.example.cmdb.it;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ServiceIT implements TestPropertyProvider {

    @Override
    public Map<String, String> getProperties() {
        // Allow overriding host/port via env vars; default to localhost:8182
        String host = Optional.ofNullable(System.getenv("CMDB_GREMLIN_HOST")).orElse("localhost");
        String portStr = Optional.ofNullable(System.getenv("CMDB_GREMLIN_PORT")).orElse("8182");

        Map<String, String> m = new HashMap<>();
        m.put("cmdb.gremlin.hosts[0]", host);
        m.put("cmdb.gremlin.port", portStr);
        m.put("cmdb.gremlin.traversalSource", "g");
        return m;
    }

    @Inject
    @Client("/")
    HttpClient httpClient;

    @BeforeAll
    void checkGremlinServerAndDataset() {
        String host = Optional.ofNullable(System.getenv("CMDB_GREMLIN_HOST")).orElse("localhost");
        int port = Integer.parseInt(Optional.ofNullable(System.getenv("CMDB_GREMLIN_PORT")).orElse("8182"));
        Cluster cluster = null;
        org.apache.tinkerpop.gremlin.driver.Client client = null;
        try {
            cluster = Cluster.build().addContactPoint(host).port(port).create();
            client = cluster.connect();
            // basic readiness
            client.submit("g.inject(1)").all().join();
            // dataset presence check against a known vertex id from cmdb-data.gremlin
            long count = client.submit("g.V().has('incident','incidentId','inc:sev1-payments-001').count()").all().join().get(0).getLong();
            if (count < 1) {
                throw new org.opentest4j.TestAbortedException(
                        "Gremlin Server reachable at " + host + ":" + port + ", but sample dataset not found. " +
                                "Please load docs/upgrade/spec/cmdb-data.gremlin before running tests.");
            }
        } catch (org.opentest4j.TestAbortedException tae) {
            throw tae;
        } catch (Exception e) {
            throw new org.opentest4j.TestAbortedException(
                    "Gremlin Server not reachable at " + host + ":" + port + ". Please start it and load docs/upgrade/spec/cmdb-data.gremlin.", e);
        } finally {
            if (client != null) try {
                client.close();
            } catch (Exception ignore) {
            }
            if (cluster != null) try {
                cluster.close();
            } catch (Exception ignore) {
            }
        }
    }

    // Helper to issue GET and decode as List of Maps
    private List<Map<String, Object>> getList(String path) {
        io.micronaut.core.type.Argument<List<Map<String, Object>>> arg = (io.micronaut.core.type.Argument<List<Map<String, Object>>>) (io.micronaut.core.type.Argument) io.micronaut.core.type.Argument.listOf(Map.class);
        HttpResponse<List<Map<String, Object>>> resp = (HttpResponse) httpClient.toBlocking().exchange(HttpRequest.GET(path), arg);
        assertThat(resp.code()).isEqualTo(200);
        List<Map<String, Object>> list = resp.body();
        return list == null ? Collections.emptyList() : list;
    }

    private Map<String, Object> getJson(String path) {
        io.micronaut.core.type.Argument<Map<String, Object>> arg = (io.micronaut.core.type.Argument<Map<String, Object>>) (io.micronaut.core.type.Argument) io.micronaut.core.type.Argument.mapOf(String.class, Object.class);
        HttpResponse<Map<String, Object>> resp = (HttpResponse) httpClient.toBlocking().exchange(HttpRequest.GET(path), arg);
        assertThat(resp.code()).isEqualTo(200);
        return resp.body();
    }

    private static boolean anyHas(List<Map<String, Object>> rows, String key, String expected) {
        for (Map<String, Object> m : rows) {
            Object v = m.get(key);
            if (v instanceof Collection) {
                Collection<?> c = (Collection<?>) v;
                if (c.stream().anyMatch(o -> expected.equals(String.valueOf(o)))) return true;
            } else if (v != null && expected.equals(String.valueOf(v))) {
                return true;
            }
        }
        return false;
    }

    @Test
    @Order(1)
    void smoke_ready() {
        HttpResponse<String> resp = httpClient.toBlocking().exchange(HttpRequest.GET("/health/ready"), String.class);
        assertThat(resp.code()).isEqualTo(200);
        assertThat(resp.body()).contains("ready");
    }

    @Test
    @Order(10)
    void svc_orders_depth2() {
        List<Map<String, Object>> rows = getList("/services/svc:orders/dependencies?depth=2");
        assertThat(rows).hasSize(2);
        assertThat(anyHas(rows, "serviceId", "svc:orders")).isTrue();
        assertThat(anyHas(rows, "depId", "dep:orders-db")).isTrue();
    }

    @Test
    @Order(11)
    void svc_payments_depth1() {
        List<Map<String, Object>> rows = getList("/services/svc:payments/dependencies?depth=1");
        assertThat(rows).hasSize(3);
        assertThat(anyHas(rows, "serviceId", "svc:payments")).isTrue();
        assertThat(anyHas(rows, "depId", "dep:payments-db")).isTrue();
        assertThat(anyHas(rows, "depId", "dep:payments-queue")).isTrue();
    }

    @Test
    @Order(12)
    void svc_inventory_depth3() {
        List<Map<String, Object>> rows = getList("/services/svc:inventory/dependencies?depth=3");
        assertThat(rows).hasSize(2);
        assertThat(anyHas(rows, "serviceId", "svc:inventory")).isTrue();
        assertThat(anyHas(rows, "depId", "dep:inventory-cache")).isTrue();
    }

    @Test
    @Order(20)
    void owners_payments() {
        List<Map<String, Object>> rows = getList("/services/svc:payments/owners");
        assertThat(rows).hasSize(1);
        assertThat(anyHas(rows, "teamId", "team:payments")).isTrue();
        assertThat(anyHas(rows, "name", "Payments")).isTrue();
        // onCall is a boolean; valueMap(true) may return as a scalar or list depending on provider
        assertThat(rows.stream().anyMatch(m -> {
            Object v = m.get("onCall");
            if (v instanceof Collection) return ((Collection<?>) v).contains(Boolean.TRUE);
            return Boolean.TRUE.equals(v);
        })).isTrue();
    }

    @Test
    @Order(21)
    void owners_api_gateway() {
        List<Map<String, Object>> rows = getList("/services/svc:api-gateway/owners");
        assertThat(rows).hasSize(1);
        assertThat(anyHas(rows, "teamId", "team:platform")).isTrue();
        assertThat(anyHas(rows, "name", "Platform Engineering")).isTrue();
    }

    @Test
    @Order(30)
    void timeline_payments_20131113_2() {
        List<Map<String, Object>> rows = getList("/deployments/depoy:payments-20231113.2/timeline");
        assertThat(rows).hasSize(1);
        assertThat(anyHas(rows, "incidentId", "inc:sev1-payments-001")).isTrue();
    }

    @Test
    @Order(31)
    void timeline_payments_20131114_1() {
        List<Map<String, Object>> rows = getList("/deployments/depoy:payments-20231114.1/timeline");
        assertThat(rows).hasSize(1);
        assertThat(anyHas(rows, "incidentId", "inc:sev1-payments-001")).isTrue();
    }

    @Test
    @Order(32)
    void timeline_api_gateway_20131114_1() {
        List<Map<String, Object>> rows = getList("/deployments/depoy:api-gateway-20231114.1/timeline");
        assertThat(rows).hasSize(1);
        assertThat(anyHas(rows, "incidentId", "inc:sev1-api-001")).isTrue();
    }

    @Test
    @Order(40)
    void blast_radius_payments_depth1() {
        Map<String, Object> body = getJson("/incidents/inc:sev1-payments-001/blastRadius?depth=1");
        @SuppressWarnings("unchecked") List<List<Map<String, Object>>> paths = (List<List<Map<String, Object>>>) body.get("paths");
        assertThat(paths).isNotNull();
        assertThat(paths.size()).isGreaterThanOrEqualTo(2);
        List<Map<String, Object>> terminals = paths.stream().map(p -> p.get(p.size() - 1)).collect(Collectors.toList());
        assertThat(anyHas(terminals, "depId", "dep:payments-db")).isTrue();
        assertThat(anyHas(terminals, "depId", "dep:payments-queue")).isTrue();
        assertThat(anyHas(terminals, "deployId", "depoy:payments-20231113.2")).isTrue();
        assertThat(anyHas(terminals, "deployId", "depoy:payments-20231114.1")).isTrue();
    }

    @Test
    @Order(41)
    void blast_radius_api_depth1() {
        Map<String, Object> body = getJson("/incidents/inc:sev1-api-001/blastRadius?depth=1");
        @SuppressWarnings("unchecked") List<List<Map<String, Object>>> paths = (List<List<Map<String, Object>>>) body.get("paths");
        assertThat(paths).isNotNull();
        List<Map<String, Object>> terminals = paths.stream().map(p -> p.get(p.size() - 1)).collect(Collectors.toList());
        assertThat(anyHas(terminals, "deployId", "depoy:api-gateway-20231113.1")).isTrue();
        assertThat(anyHas(terminals, "deployId", "depoy:api-gateway-20231114.1")).isTrue();
    }

    @Test
    @Order(50)
    void unknown_service_returns_empty() {
        List<Map<String, Object>> rows = getList("/services/svc:does-not-exist/dependencies?depth=2");
        assertThat(rows).isEmpty();
    }

    @Test
    @Order(51)
    void optional_dependency_inventory_depth1_exact_two() {
        List<Map<String, Object>> rows = getList("/services/svc:inventory/dependencies?depth=1");
        assertThat(rows).hasSize(2);
        assertThat(anyHas(rows, "serviceId", "svc:inventory")).isTrue();
        assertThat(anyHas(rows, "depId", "dep:inventory-cache")).isTrue();
    }
}
