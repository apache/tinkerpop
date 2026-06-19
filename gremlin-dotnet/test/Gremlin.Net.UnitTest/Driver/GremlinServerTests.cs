#region License

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#endregion

using System;
using Gremlin.Net.Driver;
using Xunit;

namespace Gremlin.Net.UnitTest.Driver
{
    public class GremlinServerTests
    {
        [Theory]
        [InlineData("localhost", 8182)]
        [InlineData("1.2.3.4", 5678)]
        public void ShouldBuildCorrectUri(string host, int port)
        {
            var gremlinServer = new GremlinServer(host, port);

            var uri = gremlinServer.Uri;

            Assert.Equal($"http://{host}:{port}/gremlin", uri.AbsoluteUri);
        }

        [Fact]
        public void ShouldBuildCorrectUriForSsl()
        {
            var host = "localhost";
            var port = 8181;
            var gremlinServer = new GremlinServer(host, port, true);

            var uri = gremlinServer.Uri;

            Assert.Equal($"https://{host}:{port}/gremlin", uri.AbsoluteUri);
        }

        [Fact]
        public void ShouldUseCorrectDefaultPortWhenNoneProvided()
        {
            var host = "testHost";
            var gremlinServer = new GremlinServer(host);

            var uri = gremlinServer.Uri;

            Assert.Equal(8182, uri.Port);
        }

        [Fact]
        public void ShouldUseCustomPath()
        {
            var gremlinServer = new GremlinServer("localhost", 8182, path: "/custom");

            var uri = gremlinServer.Uri;

            Assert.Equal("http://localhost:8182/custom", uri.AbsoluteUri);
        }

        [Fact]
        public void ShouldUseDefaultGremlinPath()
        {
            var gremlinServer = new GremlinServer("localhost", 8182);

            Assert.Equal("/gremlin", gremlinServer.Uri.AbsolutePath);
        }

        [Fact]
        public void ShouldBuildFromHttpUrl()
        {
            var gremlinServer = GremlinServer.FromUrl("http://example.com:8182/gremlin");

            var uri = gremlinServer.Uri;

            Assert.Equal("http", uri.Scheme);
            Assert.Equal("example.com", uri.Host);
            Assert.Equal(8182, uri.Port);
            Assert.Equal("/gremlin", uri.AbsolutePath);
        }

        [Fact]
        public void ShouldBuildFromHttpsUrl()
        {
            var gremlinServer = GremlinServer.FromUrl("https://example.com:8182/gremlin");

            var uri = gremlinServer.Uri;

            Assert.Equal("https", uri.Scheme);
            Assert.Equal("example.com", uri.Host);
            Assert.Equal(8182, uri.Port);
            Assert.Equal("/gremlin", uri.AbsolutePath);
        }

        [Fact]
        public void ShouldBuildFromUrlWithCustomPath()
        {
            var gremlinServer = GremlinServer.FromUrl("https://example.com:8182/custom/path");

            var uri = gremlinServer.Uri;

            Assert.Equal("/custom/path", uri.AbsolutePath);
            Assert.Equal("https://example.com:8182/custom/path", uri.AbsoluteUri);
        }

        [Theory]
        [InlineData("http://example.com:8182/gremlin", "example.com", 8182)]
        [InlineData("https://1.2.3.4:5678/gremlin", "1.2.3.4", 5678)]
        public void ShouldExtractHostAndPortFromUrl(string url, string expectedHost, int expectedPort)
        {
            var gremlinServer = GremlinServer.FromUrl(url);

            Assert.Equal(expectedHost, gremlinServer.Uri.Host);
            Assert.Equal(expectedPort, gremlinServer.Uri.Port);
        }

        [Fact]
        public void ShouldParseSecureUrlWithExplicitPortAndPath()
        {
            var uri = GremlinServer.FromUrl("https://h:1234/p").Uri;

            Assert.Equal("https", uri.Scheme);
            Assert.Equal("h", uri.Host);
            Assert.Equal(1234, uri.Port);
            Assert.Equal("/p", uri.AbsolutePath);
        }

        [Fact]
        public void ShouldParseInsecureUrlWithExplicitPortAndPath()
        {
            var uri = GremlinServer.FromUrl("http://h:1234/p").Uri;

            Assert.Equal("http", uri.Scheme);
            Assert.Equal("h", uri.Host);
            Assert.Equal(1234, uri.Port);
            Assert.Equal("/p", uri.AbsolutePath);
        }

        [Fact]
        public void ShouldDefaultPortWhenUrlOmitsPort()
        {
            var uri = GremlinServer.FromUrl("https://host/gremlin").Uri;

            Assert.Equal("https", uri.Scheme);
            Assert.Equal("host", uri.Host);
            Assert.Equal(8182, uri.Port);
            Assert.Equal("/gremlin", uri.AbsolutePath);
        }

        [Fact]
        public void ShouldDefaultPathWhenUrlOmitsPath()
        {
            var uri = GremlinServer.FromUrl("https://host:8182").Uri;

            Assert.Equal("https", uri.Scheme);
            Assert.Equal("host", uri.Host);
            Assert.Equal(8182, uri.Port);
            Assert.Equal("/gremlin", uri.AbsolutePath);
        }

        [Fact]
        public void ShouldDefaultPortAndPathWhenUrlOmitsBoth()
        {
            var uri = GremlinServer.FromUrl("https://host").Uri;

            Assert.Equal("https", uri.Scheme);
            Assert.Equal("host", uri.Host);
            Assert.Equal(8182, uri.Port);
            Assert.Equal("/gremlin", uri.AbsolutePath);
        }

        [Fact]
        public void ShouldRejectUnsupportedSchemeForUrl()
        {
            Assert.Throws<ArgumentException>(() => GremlinServer.FromUrl("ws://example.com:8182/gremlin"));
        }

        [Fact]
        public void ShouldRejectNonAbsoluteUrl()
        {
            Assert.Throws<ArgumentException>(() => GremlinServer.FromUrl("example.com:8182/gremlin"));
        }

        [Fact]
        public void ShouldRejectNullUrl()
        {
            Assert.Throws<ArgumentNullException>(() => GremlinServer.FromUrl(null!));
        }

        [Fact]
        public void ShouldBuildFromUri()
        {
            var gremlinServer = new GremlinServer(new Uri("https://example.com:8182/gremlin"));

            Assert.Equal("https://example.com:8182/gremlin", gremlinServer.Uri.AbsoluteUri);
        }

        [Fact]
        public void ShouldRejectUriWithUnsupportedScheme()
        {
            Assert.Throws<ArgumentException>(() => new GremlinServer(new Uri("ftp://example.com:8182/gremlin")));
        }
    }
}
