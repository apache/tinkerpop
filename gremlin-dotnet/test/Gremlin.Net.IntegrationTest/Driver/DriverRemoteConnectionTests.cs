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
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;
using Gremlin.Net.Structure.IO.GraphBinary4;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Driver;

public class DriverRemoteConnectionTests
{
    private static readonly string TestHost = ConfigProvider.Configuration["TestServerIpAddress"]!;
    private static readonly int TestPort = Convert.ToInt32(ConfigProvider.Configuration["TestServerPort"]);
        
    [Fact]
    public async Task ShouldLogWithProvidedLoggerFactory()
    {
        var loggerFactory = Substitute.For<ILoggerFactory>();
        var logger = Substitute.For<ILogger>();
        logger.IsEnabled(Arg.Any<LogLevel>()).Returns(true);
        loggerFactory.CreateLogger(Arg.Any<string>()).Returns(logger);
        using var driverRemoteConnection = new DriverRemoteConnection(TestHost, TestPort, loggerFactory: loggerFactory);
        var gremlinLangToLog = SomeValidGremlinLang;

        await driverRemoteConnection.SubmitAsync<object, object>(gremlinLangToLog);
        
        logger.VerifyMessageWasLogged(LogLevel.Debug, gremlinLangToLog.ToString());
    }
    
    [Fact]
    public async Task ShouldNotLogForDisabledLogLevel()
    {
        var loggerFactory = Substitute.For<ILoggerFactory>();
        var logger = Substitute.For<ILogger>();
        logger.IsEnabled(Arg.Any<LogLevel>()).Returns(false);
        loggerFactory.CreateLogger(Arg.Any<string>()).Returns(logger);
        using var driverRemoteConnection = new DriverRemoteConnection(TestHost, TestPort, loggerFactory: loggerFactory);

        await driverRemoteConnection.SubmitAsync<object, object>(SomeValidGremlinLang);
        
        logger.VerifyNothingWasLogged();
    }
            
    [Fact]
    public async Task ShouldLogWithLoggerFactoryFromGremlinClient()
    {
        var loggerFactory = Substitute.For<ILoggerFactory>();
        var logger = Substitute.For<ILogger>();
        logger.IsEnabled(Arg.Any<LogLevel>()).Returns(true);
        loggerFactory.CreateLogger(Arg.Any<string>()).Returns(logger);
        using var gremlinClient =
            new GremlinClient(new GremlinServer(TestHost, TestPort), loggerFactory: loggerFactory);
        var driverRemoteConnection = new DriverRemoteConnection(gremlinClient);
        var gremlinLangToLog = SomeValidGremlinLang;

        await driverRemoteConnection.SubmitAsync<object, object>(SomeValidGremlinLang);

        logger.VerifyMessageWasLogged(LogLevel.Debug, gremlinLangToLog.ToString());
    }

    private static GremlinLang SomeValidGremlinLang
    {
        get
        {
            var gremlinLang = new GremlinLang();
            gremlinLang.AddStep("inject", 1, 2);
            return gremlinLang;
        }
    }

    [Fact]
    public void ShouldRoundTripPdtViaTraversalApi()
    {
        var gremlinServer = new GremlinServer(TestHost, TestPort);
        using var gremlinClient = new GremlinClient(gremlinServer);
        using var connection = new DriverRemoteConnection(gremlinClient, "gmodern");
        var g = AnonymousTraversalSource.Traversal().With(connection);

        var pdt = new ProviderDefinedType("TestPoint",
            new Dictionary<string, object?> { { "x", 1 }, { "y", 2 } });

        var results = g.Inject<object>(pdt).ToList();

        Assert.Single(results);
        var result = Assert.IsType<ProviderDefinedType>(results[0]);
        Assert.Equal("TestPoint", result.Name);
        Assert.Equal(1, result.Fields["x"]);
        Assert.Equal(2, result.Fields["y"]);
    }

    [Fact]
    public void ShouldRoundTripTypedObjectViaRegistry()
    {
        var registry = new ProviderDefinedTypeRegistry();
        registry.Register(new TestPointAdapter());

        var gremlinServer = new GremlinServer(TestHost, TestPort);
        using var gremlinClient = new GremlinClient(gremlinServer, pdtRegistry: registry);
        using var connection = new DriverRemoteConnection(gremlinClient, "gmodern", pdtRegistry: registry);
        var g = AnonymousTraversalSource.Traversal().With(connection);

        var point = new TestPointClass { X = 5, Y = 10 };

        var results = g.Inject<object>(point).ToList();

        Assert.Single(results);
        var result = Assert.IsType<TestPointClass>(results[0]);
        Assert.Equal(5, result.X);
        Assert.Equal(10, result.Y);
    }

    [Fact]
    public void ShouldRoundTripAnnotatedClass()
    {
        var gremlinServer = new GremlinServer(TestHost, TestPort);
        using var gremlinClient = new GremlinClient(gremlinServer);
        using var connection = new DriverRemoteConnection(gremlinClient, "gmodern");
        var g = AnonymousTraversalSource.Traversal().With(connection);

        var point = new AnnotatedTestPoint { X = 3, Y = 7 };

        var results = g.Inject<object>(point).ToList();

        Assert.Single(results);
        var result = Assert.IsType<AnnotatedTestPoint>(results[0]);
        Assert.Equal(3, result.X);
        Assert.Equal(7, result.Y);
    }

    #region Test helpers

    private class TestPointClass
    {
        public int X { get; set; }
        public int Y { get; set; }
    }

    private class TestPointAdapter : IProviderDefinedTypeAdapter<TestPointClass>
    {
        public string TypeName => "TestPoint";

        public TestPointClass FromFields(IReadOnlyDictionary<string, object?> fields)
        {
            return new TestPointClass
            {
                X = Convert.ToInt32(fields["x"]),
                Y = Convert.ToInt32(fields["y"])
            };
        }

        public IReadOnlyDictionary<string, object?> ToFields(TestPointClass obj)
        {
            return new ReadOnlyDictionary<string, object?>(
                new Dictionary<string, object?> { { "x", obj.X }, { "y", obj.Y } });
        }
    }

    [ProviderDefined(Name = "TestPoint")]
    private class AnnotatedTestPoint
    {
        public int X { get; set; }
        public int Y { get; set; }
    }

    #endregion

    [Fact]
    public void ShouldRoundTripPrimitivePdtViaTraversalApi()
    {
        var gremlinServer = new GremlinServer(TestHost, TestPort);
        using var gremlinClient = new GremlinClient(gremlinServer);
        using var connection = new DriverRemoteConnection(gremlinClient, "gmodern");
        var g = AnonymousTraversalSource.Traversal().With(connection);

        var pdt = new PrimitiveProviderDefinedType("TestToken", "abc123");

        var results = g.Inject<object>(pdt).ToList();

        Assert.Single(results);
        var result = Assert.IsType<PrimitiveProviderDefinedType>(results[0]);
        Assert.Equal("TestToken", result.Name);
        Assert.Equal("abc123", result.Value);
    }

    [Fact]
    public void ShouldRoundTripPrimitiveTypedObjectViaRegistry()
    {
        var registry = new ProviderDefinedTypeRegistry();
        registry.RegisterPrimitive(new TestUint32Adapter());

        var gremlinServer = new GremlinServer(TestHost, TestPort);
        using var gremlinClient = new GremlinClient(gremlinServer, pdtRegistry: registry);
        using var connection = new DriverRemoteConnection(gremlinClient, "gmodern", pdtRegistry: registry);
        var g = AnonymousTraversalSource.Traversal().With(connection);

        var val = 42u;

        var results = g.Inject<object>(val).ToList();

        Assert.Single(results);
        Assert.IsType<uint>(results[0]);
        Assert.Equal(42u, (uint)results[0]);
    }

    #region Test helpers (primitive)

    private class TestUint32Adapter : IPrimitivePdtAdapter<uint>
    {
        public string TypeName => "TestToken";
        public uint FromString(string value) => uint.Parse(value);
        public string ToString(uint obj) => obj.ToString();
    }

    #endregion
}
