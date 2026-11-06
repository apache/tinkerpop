/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package gremlingo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func socketServerURL() string {
	return getEnvOrDefaultString("GREMLIN_SOCKET_SERVER_URL",
		fmt.Sprintf("http://localhost:%d/gremlin", socketServerPort))
}

func socketServerProxyURL() string {
	return getEnvOrDefaultString("GREMLIN_SOCKET_SERVER_PROXY_URL",
		"http://localhost:45944")
}

// resetProxyRecording clears the proxy's recorded targets via POST /__reset.
func resetProxyRecording(t *testing.T) {
	t.Helper()
	proxyURL := socketServerProxyURL()
	resp, err := http.Post(proxyURL+"/__reset", "application/json", nil)
	if err != nil {
		t.Skipf("Recording proxy not available: %v", err)
	}
	resp.Body.Close()
}

// recordedProxyTargets fetches the recorded "host:port" targets via GET /__recorded.
func recordedProxyTargets(t *testing.T) []string {
	t.Helper()
	proxyURL := socketServerProxyURL()
	resp, err := http.Get(proxyURL + "/__recorded")
	if err != nil {
		t.Skipf("Recording proxy not available: %v", err)
	}
	defer resp.Body.Close()

	var recorded []string
	if err := json.NewDecoder(resp.Body).Decode(&recorded); err != nil {
		t.Fatalf("failed to decode recorded proxy targets: %v", err)
	}
	return recorded
}

// anyTargetHasPortSuffix reports whether any recorded target ends with ":<port>",
// matching on port suffix only so the host portion is ignored.
func anyTargetHasPortSuffix(targets []string, port int) bool {
	suffix := fmt.Sprintf(":%d", port)
	for _, target := range targets {
		if strings.HasSuffix(target, suffix) {
			return true
		}
	}
	return false
}

// TestProxyRoutesSocketServerTraffic verifies that traffic submitted through a
// client configured with a Proxy is routed to the socket server via the shared
// recording proxy, and that the proxy records the socket server target.
func TestProxyRoutesSocketServerTraffic(t *testing.T) {
	resetProxyRecording(t)

	parsedProxyURL, err := url.Parse(socketServerProxyURL())
	require.NoError(t, err)

	client, err := NewClient(socketServerURL(), func(settings *ClientSettings) {
		settings.Proxy = func(*http.Request) (*url.URL, error) {
			return parsedProxyURL, nil
		}
	})
	require.NoError(t, err)
	defer client.Close()

	rs, err := client.Submit(gremlinSingleVertex)
	require.NoError(t, err)
	results, err := rs.All()
	require.NoError(t, err)
	assert.Equal(t, 1, len(results))

	targets := recordedProxyTargets(t)
	assert.True(t, anyTargetHasPortSuffix(targets, socketServerPort),
		"expected at least one recorded target ending with :%d, got %v", socketServerPort, targets)
}

// TestProxyNegativeControl verifies that a client connecting directly to the
// socket server (no Proxy configured) does not route through the recording
// proxy, so the proxy records no socket server target.
func TestProxyNegativeControl(t *testing.T) {
	resetProxyRecording(t)

	client, err := NewClient(socketServerURL(), func(settings *ClientSettings) {
		// Force a direct connection (no proxy) regardless of ambient proxy env vars.
		settings.Proxy = func(*http.Request) (*url.URL, error) {
			return nil, nil
		}
	})
	require.NoError(t, err)
	defer client.Close()

	rs, err := client.Submit(gremlinSingleVertex)
	require.NoError(t, err)
	results, err := rs.All()
	require.NoError(t, err)
	assert.Equal(t, 1, len(results))

	targets := recordedProxyTargets(t)
	assert.False(t, anyTargetHasPortSuffix(targets, socketServerPort),
		"expected no recorded target ending with :%d, got %v", socketServerPort, targets)
}

func newSocketServerClient(t *testing.T, configurations ...func(*ClientSettings)) *Client {
	t.Helper()
	url := socketServerURL()
	client, err := NewClient(url, configurations...)
	if err != nil {
		t.Skipf("Socket server not available: %v", err)
	}
	// Verify connectivity
	_, submitErr := client.Submit(gremlinSingleVertex)
	if submitErr != nil {
		client.Close()
		t.Skip("Socket server not available")
	}
	return client
}

func assertRecovery(t *testing.T, client *Client) {
	t.Helper()
	rs, err := client.Submit(gremlinSingleVertex)
	assert.NoError(t, err)
	results, err := rs.All()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(results))
}

// submitExpectErr submits the gremlin string and returns the effective error.
// Errors may surface either from Submit() or from reading the ResultSet via All().
func submitExpectErr(client *Client, gremlin string) error {
	rs, err := client.Submit(gremlin)
	if err != nil {
		return err
	}
	_, err = rs.All()
	return err
}

func TestShouldReceiveSingleVertex(t *testing.T) {
	client := newSocketServerClient(t)
	defer client.Close()

	rs, err := client.Submit(gremlinSingleVertex)
	require.NoError(t, err)
	results, err := rs.All()
	require.NoError(t, err)
	assert.Equal(t, 1, len(results))
}

func TestShouldHandleServerClosingConnectionBeforeResponse(t *testing.T) {
	client := newSocketServerClient(t)
	defer client.Close()

	err := submitExpectErr(client, gremlinCloseConnection)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "EOF")

	assertRecovery(t, client)
}

func TestShouldHandleServerClosingConnectionAfterResponse(t *testing.T) {
	client := newSocketServerClient(t)
	defer client.Close()

	rs, err := client.Submit(gremlinVertexThenClose)
	require.NoError(t, err)
	results, err := rs.All()
	require.NoError(t, err)
	assert.Equal(t, 1, len(results))

	time.Sleep(3 * time.Second)

	assertRecovery(t, client)
}

func TestShouldHandleServerErrorAfterDelay(t *testing.T) {
	client := newSocketServerClient(t)
	defer client.Close()

	err := submitExpectErr(client, gremlinFailAfterDelay)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "500")

	assertRecovery(t, client)
}

func TestShouldHandlePartialContentClose(t *testing.T) {
	client := newSocketServerClient(t)
	defer client.Close()

	err := submitExpectErr(client, gremlinPartialContentClose)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected EOF")

	assertRecovery(t, client)
}

func TestShouldHandleMalformedResponse(t *testing.T) {
	client := newSocketServerClient(t)
	defer client.Close()

	err := submitExpectErr(client, gremlinMalformedResponse)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "deserialize")

	assertRecovery(t, client)
}

func TestShouldHandleEmptyResponseBody(t *testing.T) {
	url := socketServerURL()
	client, err := NewClient(url, func(settings *ClientSettings) {
		settings.ConnectTimeout = 5 * time.Second
	})
	if err != nil {
		t.Skip("Socket server not available")
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- submitExpectErr(client, gremlinEmptyBody)
	}()

	select {
	case submitErr := <-done:
		require.Error(t, submitErr)
		assert.Contains(t, submitErr.Error(), "empty response body")
	case <-ctx.Done():
		t.Fatal("request hung on empty response body")
	}

	assertRecovery(t, client)
}

func TestShouldHandleSlowResponse(t *testing.T) {
	client := newSocketServerClient(t)
	defer client.Close()

	rs, err := client.Submit(gremlinSlowResponse)
	require.NoError(t, err)
	results, err := rs.All()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(results), 1)
}

func TestShouldTimeoutWhenServerNeverResponds(t *testing.T) {
	url := socketServerURL()
	client, err := NewClient(url, func(settings *ClientSettings) {
		settings.ReadTimeout = 2 * time.Second
	})
	if err != nil {
		t.Skip("Socket server not available")
	}
	defer client.Close()

	// Verify connectivity before testing the no-response scenario
	if err := submitExpectErr(client, gremlinSingleVertex); err != nil {
		t.Skip("Socket server not available")
	}

	err = submitExpectErr(client, gremlinNoResponse)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
}

func TestShouldHandleAsyncRequestsDuringConnectionClose(t *testing.T) {
	client := newSocketServerClient(t)
	defer client.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			err := submitExpectErr(client, gremlinCloseConnection)
			assert.Error(t, err)
		}()
	}

	wg.Wait()

	assertRecovery(t, client)
}

func TestShouldHandleConcurrentMixedRequests(t *testing.T) {
	client := newSocketServerClient(t)
	defer client.Close()

	var wg sync.WaitGroup
	goodResults := make([]error, 5)
	badResults := make([]error, 5)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			rs, err := client.Submit(gremlinSingleVertex)
			if err != nil {
				goodResults[idx] = err
				return
			}
			_, goodResults[idx] = rs.All()
		}(i)
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			badResults[idx] = submitExpectErr(client, gremlinCloseConnection)
		}(i)
	}

	wg.Wait()

	for i, err := range goodResults {
		assert.NoError(t, err, "good request %d should succeed", i)
	}
	for i, err := range badResults {
		assert.Error(t, err, "bad request %d should fail", i)
	}
}
