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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
using System.Text;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.Runtime.Internal;
using Amazon.Runtime.Internal.Auth;
using Amazon.Runtime.Internal.Util;

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     Provides factory methods for built-in request interceptors.
    /// </summary>
    public static class Auth
    {
        /// <summary>
        ///     Returns a request interceptor that adds an HTTP Basic Authentication header.
        ///     The credentials are pre-computed once and set on every request.
        /// </summary>
        /// <param name="username">The username.</param>
        /// <param name="password">The password.</param>
        /// <returns>A request interceptor delegate.</returns>
        public static Func<HttpRequestContext, Task> BasicAuth(string username, string password)
        {
            var encoded = Convert.ToBase64String(
                Encoding.UTF8.GetBytes(username + ":" + password));
            var headerValue = "Basic " + encoded;

            return context =>
            {
                context.Headers["Authorization"] = headerValue;
                return Task.CompletedTask;
            };
        }

        /// <summary>
        ///     Returns a request interceptor that signs requests using AWS Signature Version 4.
        ///     If <paramref name="credentials"/> is null, the default AWS credential chain is
        ///     used and the resolved provider is cached on first use (the provider itself handles
        ///     credential refresh for expiring credentials like STS).
        /// </summary>
        /// <param name="region">The AWS region (e.g. "us-east-1").</param>
        /// <param name="service">The AWS service name (e.g. "neptune-db").</param>
        /// <param name="credentials">
        ///     Optional AWS credentials. When null, the default credential chain is used.
        /// </param>
        /// <returns>A request interceptor delegate.</returns>
        public static Func<HttpRequestContext, Task> SigV4Auth(
            string region, string service, AWSCredentials? credentials = null)
        {
            // Cache the credential provider once when using the default chain.
            AWSCredentials? cachedProvider = credentials;
            var cacheLock = new object();

            // Create signer and config once — both are stateless and thread-safe
            var signer = new AWS4Signer();
            var clientConfig = new SigningClientConfig
            {
                AuthenticationRegion = region,
                AuthenticationServiceName = service,
            };

            return async context =>
            {
                if (cachedProvider == null)
                {
                    lock (cacheLock)
                    {
                        // FallbackCredentialsFactory only has a sync API, but this runs once.
                        cachedProvider ??= FallbackCredentialsFactory.GetCredentials();
                    }
                }

                // Use the async path — important for credential providers that perform
                // network I/O (e.g. IMDS on EC2, ECS task role endpoint).
                var immutableCreds = await cachedProvider.GetCredentialsAsync()
                    .ConfigureAwait(false);
                SignRequest(context, immutableCreds, signer, clientConfig);
            };
        }

        private static void SignRequest(HttpRequestContext context,
            ImmutableCredentials credentials, AWS4Signer signer, SigningClientConfig clientConfig)
        {
            // Build a DefaultRequest from the HttpRequestContext for the AWS SDK signer.
            var endpointUri = new Uri(context.Uri.GetLeftPart(UriPartial.Authority));
            var awsRequest = new DefaultRequest(new NullRequest(), clientConfig.AuthenticationServiceName)
            {
                HttpMethod = context.Method,
                Endpoint = endpointUri,
                ResourcePath = context.Uri.AbsolutePath,
                Content = context.Body is byte[] bytes
                    ? bytes
                    : throw new InvalidOperationException(
                        "SigV4 signing requires Body to be byte[]. " +
                        "Ensure serialization occurs before the SigV4 interceptor."),
                AuthenticationRegion = clientConfig.AuthenticationRegion,
                OverrideSigningServiceName = clientConfig.AuthenticationServiceName,
            };

            // Copy headers (skip Host — signer adds it)
            foreach (var header in context.Headers)
            {
                if (!string.Equals(header.Key, "Host", StringComparison.OrdinalIgnoreCase))
                {
                    awsRequest.Headers[header.Key] = header.Value;
                }
            }

            // Copy query parameters
            var query = context.Uri.Query;
            if (!string.IsNullOrEmpty(query))
            {
                // Remove leading '?'
                var queryString = query.StartsWith("?") ? query.Substring(1) : query;
                foreach (var param in queryString.Split('&'))
                {
                    if (string.IsNullOrEmpty(param)) continue;
                    var parts = param.Split(new[] { '=' }, 2);
                    var key = Uri.UnescapeDataString(parts[0]);
                    var value = parts.Length > 1 ? Uri.UnescapeDataString(parts[1]) : "";
                    awsRequest.Parameters[key] = value;
                }
            }

            // Set content hash header before signing
            var payloadHash = context.GetPayloadHash();
            awsRequest.Headers["x-amz-content-sha256"] = payloadHash;

            // Sign the request
            signer.Sign(awsRequest, clientConfig, new RequestMetrics(), credentials);

            // Copy signed headers back to context. Cherry-pick the known SigV4 headers
            // because the .NET Dictionary is case-sensitive and the AWS SDK may use
            // different casing than what interceptors expect.
            context.Headers["Host"] = endpointUri.Host;
            if (awsRequest.Headers.ContainsKey("Authorization"))
            {
                context.Headers["Authorization"] = awsRequest.Headers["Authorization"];
            }
            if (awsRequest.Headers.ContainsKey("X-Amz-Date"))
            {
                context.Headers["X-Amz-Date"] = awsRequest.Headers["X-Amz-Date"];
            }
            context.Headers["x-amz-content-sha256"] = payloadHash;

            // Add session token if temporary credentials
            if (!string.IsNullOrEmpty(credentials.Token))
            {
                context.Headers["X-Amz-Security-Token"] = credentials.Token;
            }
        }

        /// <summary>
        ///     A minimal AmazonWebServiceRequest implementation required by DefaultRequest.
        /// </summary>
        private class NullRequest : AmazonWebServiceRequest
        {
        }

        /// <summary>
        ///     A minimal ClientConfig implementation for SigV4 signing.
        /// </summary>
        private class SigningClientConfig : ClientConfig
        {
            public override string RegionEndpointServiceName => "execute-api";
            public override string ServiceVersion => "2024-01-01";
            public override string UserAgent => "gremlin-dotnet";
        }
    }
}
