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
using System.Net.Security;
using System.Threading;
using Gremlin.Net.Driver;
using Xunit;

namespace Gremlin.Net.UnitTest.Driver
{
    public class ConnectionSettingsTests
    {
        [Fact]
        public void ShouldUseStandardizedDefaults()
        {
            var settings = new ConnectionSettings();

            Assert.Equal(TimeSpan.FromSeconds(5), settings.ConnectTimeout);
            Assert.Equal(TimeSpan.FromSeconds(180), settings.IdleTimeout);
            Assert.Equal(128, settings.MaxConnections);
            Assert.Equal(TimeSpan.FromSeconds(30), settings.KeepAliveTime);
            Assert.Equal(64, settings.BatchSize);
            Assert.Equal(Compression.Deflate, settings.Compression);
            Assert.False(settings.SkipCertificateValidation);
            Assert.Null(settings.Ssl);
            Assert.Null(settings.Proxy);
            Assert.Equal(0, settings.MaxResponseHeaderBytes);
            Assert.Equal(Timeout.InfiniteTimeSpan, settings.ReadTimeout);
        }

        [Fact]
        public void CompressionShouldDefaultToDeflate()
        {
            Assert.Equal(CompressionType.Deflate, new ConnectionSettings().Compression.Type);
            Assert.True(new ConnectionSettings().Compression.Enabled);
        }

        [Fact]
        public void CompressionShouldAcceptEnumValue()
        {
            var settings = new ConnectionSettings { Compression = CompressionType.Deflate };

            Assert.Equal(Compression.Deflate, settings.Compression);
        }

        [Fact]
        public void CompressionEqualityShouldWork()
        {
            Assert.Equal(Compression.Deflate, Compression.Deflate);
            Assert.NotEqual(Compression.Deflate, Compression.None);
            Assert.True(Compression.None == CompressionType.None);
            Assert.True(Compression.Deflate != Compression.None);
        }

        [Fact]
        public void ShouldAllowSettingSslOptions()
        {
            var ssl = new SslClientAuthenticationOptions { TargetHost = "example.com" };
            var settings = new ConnectionSettings { Ssl = ssl };

            Assert.Same(ssl, settings.Ssl);
        }

        [Fact]
        public void MillisOptionsShouldSetTheTimeSpanProperties()
        {
            var settings = new ConnectionSettings
            {
                ConnectTimeoutMillis = 2000,
                IdleTimeoutMillis = 60000,
                KeepAliveTimeMillis = 15000,
                ReadTimeoutMillis = 30000
            };

            Assert.Equal(TimeSpan.FromMilliseconds(2000), settings.ConnectTimeout);
            Assert.Equal(TimeSpan.FromMilliseconds(60000), settings.IdleTimeout);
            Assert.Equal(TimeSpan.FromMilliseconds(15000), settings.KeepAliveTime);
            Assert.Equal(TimeSpan.FromMilliseconds(30000), settings.ReadTimeout);
        }

        [Fact]
        public void MillisOptionsShouldReflectTheTimeSpanProperties()
        {
            var settings = new ConnectionSettings
            {
                ConnectTimeout = TimeSpan.FromSeconds(2),
                ReadTimeout = TimeSpan.FromSeconds(30)
            };

            Assert.Equal(2000, settings.ConnectTimeoutMillis);
            Assert.Equal(30000, settings.ReadTimeoutMillis);
        }

        [Fact]
        public void ReadTimeoutMillisZeroShouldDisableTheReadTimeout()
        {
            var settings = new ConnectionSettings { ReadTimeoutMillis = 0 };

            Assert.Equal(Timeout.InfiniteTimeSpan, settings.ReadTimeout);
            Assert.Equal(0, settings.ReadTimeoutMillis);
        }

    }
}
