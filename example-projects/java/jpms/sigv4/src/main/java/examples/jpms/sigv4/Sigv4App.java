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
package examples.jpms.sigv4;

import org.apache.tinkerpop.gremlin.driver.HttpRequest;
import org.apache.tinkerpop.gremlin.driver.auth.Auth;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Signs an in-memory request to prove that the driver's AWS SDK dependencies resolve on the module path. No request
 * is sent to the example URI. Any failed check throws, so the process exits with a non-zero status.
 */
public final class Sigv4App {

    private static final String REGION = "service-region-1";
    private static final String SERVICE_NAME = "service-name";
    private static final String HOST = "host.example.com";
    private static final String ACCESS_KEY = "foo";
    private static final String SECRET_KEY = "bar";

    private Sigv4App() {}

    public static void main(final String[] args) {
        final Map<String, String> headers = new LinkedHashMap<>();
        final byte[] body = "{\"gremlin\":\"g.inject(1)\"}".getBytes(StandardCharsets.UTF_8);
        final HttpRequest request = new HttpRequest(headers, body,
                URI.create("https://" + HOST + "/gremlin?language=gremlin-lang"));

        Auth.sigv4(REGION, () -> AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY), SERVICE_NAME).intercept(request);

        checkHeader(headers, "host", HOST);
        checkHeaderPrefix(headers, "authorization", "AWS4-HMAC-SHA256 Credential=" + ACCESS_KEY);
        checkHeaderContains(headers, "authorization", "/" + REGION + "/" + SERVICE_NAME + "/aws4_request");
        checkHeaderPresent(headers, "x-amz-date");
        checkHeaderPresent(headers, "x-amz-content-sha256");
        if (request.getBody() != body)
            throw new IllegalStateException("SigV4 signing unexpectedly replaced the serialized request body");

        System.out.println("SigV4 example completed");
    }

    private static void checkHeader(final Map<String, String> headers, final String name, final String expected) {
        final String actual = header(headers, name);
        if (!expected.equals(actual))
            throw new IllegalStateException(name + ": expected " + expected + " but was " + actual);
    }

    private static void checkHeaderPresent(final Map<String, String> headers, final String name) {
        final String actual = header(headers, name);
        if (null == actual || actual.isEmpty())
            throw new IllegalStateException("the signed request has no " + name + " header");
    }

    private static void checkHeaderPrefix(final Map<String, String> headers, final String name, final String prefix) {
        final String actual = header(headers, name);
        if (null == actual || !actual.startsWith(prefix))
            throw new IllegalStateException(name + ": expected a value starting with " + prefix + " but was " +
                    actual);
    }

    private static void checkHeaderContains(final Map<String, String> headers, final String name,
                                            final String expected) {
        final String actual = header(headers, name);
        if (null == actual || !actual.contains(expected))
            throw new IllegalStateException(name + ": expected a value containing " + expected + " but was " +
                    actual);
    }

    private static String header(final Map<String, String> headers, final String name) {
        return headers.entrySet().stream().
                filter(entry -> entry.getKey().equalsIgnoreCase(name)).
                map(Map.Entry::getValue).
                findFirst().
                orElse(null);
    }
}
