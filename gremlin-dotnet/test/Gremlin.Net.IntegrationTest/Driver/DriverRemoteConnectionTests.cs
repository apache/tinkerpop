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
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.Process.Traversal;
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
        var bytecodeToLog = SomeValidBytecode;

        await driverRemoteConnection.SubmitAsync<object, object>(bytecodeToLog);
        
        logger.VerifyMessageWasLogged(LogLevel.Debug, bytecodeToLog.ToString());
    }
    
    [Fact]
    public async Task ShouldNotLogForDisabledLogLevel()
    {
        var loggerFactory = Substitute.For<ILoggerFactory>();
        var logger = Substitute.For<ILogger>();
        logger.IsEnabled(Arg.Any<LogLevel>()).Returns(false);
        loggerFactory.CreateLogger(Arg.Any<string>()).Returns(logger);
        using var driverRemoteConnection = new DriverRemoteConnection(TestHost, TestPort, loggerFactory: loggerFactory);

        await driverRemoteConnection.SubmitAsync<object, object>(SomeValidBytecode);
        
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
        var bytecodeToLog = SomeValidBytecode;

        await driverRemoteConnection.SubmitAsync<object, object>(SomeValidBytecode);

        logger.VerifyMessageWasLogged(LogLevel.Debug, bytecodeToLog.ToString());
    }

    private static Bytecode SomeValidBytecode
    {
        get
        {
            var bytecode = new Bytecode();
            bytecode.StepInstructions.Add(new Instruction("inject", 1, 2));
            return bytecode;
        }
    }
}