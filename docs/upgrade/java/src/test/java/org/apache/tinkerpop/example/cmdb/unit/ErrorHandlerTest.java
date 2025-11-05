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

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.hateoas.JsonError;
import org.apache.tinkerpop.example.cmdb.errors.ApiException;
import org.apache.tinkerpop.example.cmdb.errors.ErrorHandler;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ErrorHandlerTest {

    @Test
    void mapsApiExceptionToHttpResponse() {
        ErrorHandler handler = new ErrorHandler();
        ApiException ex = new ApiException(422, "bad input");

        HttpRequest<?> request = mock(HttpRequest.class);
        when(request.getUri()).thenReturn(URI.create("/test"));

        HttpResponse<JsonError> resp = handler.handle(request, ex);
        assertThat(resp.getStatus().getCode()).isEqualTo(422);
        assertThat(resp.getBody()).isPresent();
        assertThat(resp.getBody().get().getMessage()).contains("bad input");
    }
}
