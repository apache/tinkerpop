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
package org.apache.tinkerpop.example.cmdb.service;

import jakarta.inject.Singleton;
import org.apache.tinkerpop.example.cmdb.errors.ApiException;
import org.apache.tinkerpop.example.cmdb.graph.GremlinGateway;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import java.util.Map;

@Singleton
public class AlertService {

    private final GremlinGateway gateway;

    public AlertService(GremlinGateway gateway) {
        this.gateway = gateway;
    }

    public void upsertAlert(Map<String, Object> body) {
        String alertId = getString(body, "alertId");
        if (alertId == null || alertId.isBlank()) {
            throw new ApiException(400, "alertId is required");
        }
        String source = getString(body, "source");
        String metric = getString(body, "metric");
        Object firedAt = body.get("firedAt");

        gateway.iterate(g -> g.V().has("alert", "alertId", alertId).fold()
                .coalesce(__.unfold(), __.addV("alert").property("alertId", alertId))
                .property("source", source)
                .property("metric", metric)
                .property("firedAt", firedAt));
    }

    private static String getString(Map<String, Object> m, String k) {
        Object v = m.get(k);
        return v == null ? null : String.valueOf(v);
    }
}
