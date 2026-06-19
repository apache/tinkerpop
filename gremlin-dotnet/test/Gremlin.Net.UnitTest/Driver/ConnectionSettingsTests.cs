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
            Assert.Equal(64, settings.DefaultBatchSize);
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
        public void ObsoleteAliasesShouldMapToCanonicalProperties()
        {
#pragma warning disable 618
            var settings = new ConnectionSettings
            {
                MaxConnectionsPerServer = 42,
                ConnectionTimeout = TimeSpan.FromSeconds(7),
                IdleConnectionTimeout = TimeSpan.FromSeconds(99),
                KeepAliveInterval = TimeSpan.FromSeconds(11),
            };

            // Reading the obsolete alias returns the canonical value.
            Assert.Equal(42, settings.MaxConnectionsPerServer);
            Assert.Equal(TimeSpan.FromSeconds(7), settings.ConnectionTimeout);
            Assert.Equal(TimeSpan.FromSeconds(99), settings.IdleConnectionTimeout);
            Assert.Equal(TimeSpan.FromSeconds(11), settings.KeepAliveInterval);
#pragma warning restore 618

            // The canonical property reflects the same backing value.
            Assert.Equal(42, settings.MaxConnections);
            Assert.Equal(TimeSpan.FromSeconds(7), settings.ConnectTimeout);
            Assert.Equal(TimeSpan.FromSeconds(99), settings.IdleTimeout);
            Assert.Equal(TimeSpan.FromSeconds(11), settings.KeepAliveTime);
        }

        [Fact]
        public void ObsoleteAliasesShouldReflectCanonicalWrites()
        {
            var settings = new ConnectionSettings
            {
                MaxConnections = 256,
                ConnectTimeout = TimeSpan.FromSeconds(3),
                IdleTimeout = TimeSpan.FromSeconds(60),
                KeepAliveTime = TimeSpan.FromSeconds(15),
            };

#pragma warning disable 618
            Assert.Equal(256, settings.MaxConnectionsPerServer);
            Assert.Equal(TimeSpan.FromSeconds(3), settings.ConnectionTimeout);
            Assert.Equal(TimeSpan.FromSeconds(60), settings.IdleConnectionTimeout);
            Assert.Equal(TimeSpan.FromSeconds(15), settings.KeepAliveInterval);
#pragma warning restore 618
        }

        [Fact]
        public void ObsoleteEnableCompressionShouldMapToCompression()
        {
            var settings = new ConnectionSettings();

#pragma warning disable 618
            // Default is Deflate, so the bool shim reads true.
            Assert.True(settings.EnableCompression);

            settings.EnableCompression = false;
            Assert.Equal(Compression.None, settings.Compression);
            Assert.False(settings.EnableCompression);

            settings.EnableCompression = true;
            Assert.Equal(Compression.Deflate, settings.Compression);
            Assert.True(settings.EnableCompression);
#pragma warning restore 618
        }

        [Fact]
        public void ObsoleteEnableCompressionShouldReflectCanonicalCompression()
        {
            var settings = new ConnectionSettings { Compression = Compression.None };

#pragma warning disable 618
            Assert.False(settings.EnableCompression);
#pragma warning restore 618

            settings.Compression = Compression.Deflate;

#pragma warning disable 618
            Assert.True(settings.EnableCompression);
#pragma warning restore 618
        }
    }
}
