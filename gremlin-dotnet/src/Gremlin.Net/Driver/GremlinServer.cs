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

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     Represents a Gremlin Server.
    /// </summary>
    public class GremlinServer
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="GremlinServer" /> class with the specified connection parameters.
        /// </summary>
        /// <param name="hostname">The hostname of the server.</param>
        /// <param name="port">The port on which Gremlin Server can be reached.</param>
        /// <param name="enableSsl">Specifies whether SSL should be enabled.</param>
        /// <param name="path">The path to the Gremlin endpoint on the server.</param>
        public GremlinServer(string hostname = "localhost", int port = 8182, bool enableSsl = false,
            string path = "/gremlin")
        {
            Uri = CreateUri(hostname, port, enableSsl, path);
        }

        /// <summary>
        ///     Creates a new instance of the <see cref="GremlinServer" /> class from a single URL.
        /// </summary>
        /// <param name="url">
        ///     The URL of the Gremlin endpoint, e.g. <c>https://localhost:8182/gremlin</c>. The scheme determines
        ///     whether SSL is enabled (<c>https</c> enables it, <c>http</c> disables it) and the host, port and path
        ///     are taken from the URL. When the URL omits the port the default <c>8182</c> is used, and when it omits
        ///     the path the default <c>/gremlin</c> is used.
        /// </param>
        /// <returns>A new <see cref="GremlinServer" /> configured from the given URL.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="url" /> is null.</exception>
        /// <exception cref="ArgumentException">
        ///     Thrown when <paramref name="url" /> is not a valid absolute URL or does not use the
        ///     <c>http</c> or <c>https</c> scheme.
        /// </exception>
        public static GremlinServer FromUrl(string url)
        {
            if (url == null) throw new ArgumentNullException(nameof(url));
            if (!Uri.TryCreate(url, UriKind.Absolute, out var uri))
                throw new ArgumentException($"'{url}' is not a valid absolute URL.", nameof(url));
            return new GremlinServer(uri);
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="GremlinServer" /> class from a <see cref="System.Uri" />.
        /// </summary>
        /// <param name="uri">
        ///     The URI of the Gremlin endpoint. The scheme determines whether SSL is enabled (<c>https</c> enables it,
        ///     <c>http</c> disables it). When the URI omits the port the default <c>8182</c> is used, and when it omits
        ///     the path the default <c>/gremlin</c> is used.
        /// </param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="uri" /> is null.</exception>
        /// <exception cref="ArgumentException">
        ///     Thrown when <paramref name="uri" /> does not use the <c>http</c> or <c>https</c> scheme.
        /// </exception>
        public GremlinServer(Uri uri)
        {
            if (uri == null) throw new ArgumentNullException(nameof(uri));
            ValidateScheme(uri.Scheme);

            var enableSsl = string.Equals(uri.Scheme, "https", StringComparison.OrdinalIgnoreCase);

            // Only override the port when the URL specifies one; otherwise keep the default 8182.
            // System.Uri auto-fills the scheme default port (80/443) and flags it via IsDefaultPort,
            // so treat that case as "not specified".
            var port = uri.IsDefaultPort ? 8182 : uri.Port;

            // Likewise, only override the path when the URL has a non-empty path, otherwise keep the default
            // /gremlin. System.Uri turns a path-less URL into AbsolutePath "/", so treat "/" or empty as default.
            var path = string.IsNullOrEmpty(uri.AbsolutePath) || uri.AbsolutePath == "/"
                ? "/gremlin"
                : uri.AbsolutePath;

            Uri = CreateUri(uri.Host, port, enableSsl, path);
        }

        /// <summary>
        ///     Gets the URI of the Gremlin Server.
        /// </summary>
        public Uri Uri { get; }

        private static Uri CreateUri(string hostname, int port, bool enableSsl, string path)
        {
            var scheme = enableSsl ? "https" : "http";
            return new Uri($"{scheme}://{hostname}:{port}{path}");
        }

        private static void ValidateScheme(string scheme)
        {
            if (!string.Equals(scheme, "http", StringComparison.OrdinalIgnoreCase) &&
                !string.Equals(scheme, "https", StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException(
                    $"Unsupported scheme '{scheme}'. Only 'http' and 'https' are supported.");
        }
    }
}
