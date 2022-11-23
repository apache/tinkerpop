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
using Moq;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Driver;

public class DriverRemoteConnectionTests
{
    private static readonly string TestHost = ConfigProvider.Configuration["TestServerIpAddress"]!;
    private static readonly int TestPort = Convert.ToInt32(ConfigProvider.Configuration["TestServerPort"]);
        
    [Fact]
    public async Task ShouldLogWithProvidedLoggerFactory()
    {
        var loggerFactoryMock = new Mock<ILoggerFactory>();
        var loggerMock = new Mock<ILogger>();
        loggerMock.Setup(m => m.IsEnabled(It.IsAny<LogLevel>())).Returns(true);
        loggerFactoryMock.Setup(m => m.CreateLogger(It.IsAny<string>())).Returns(loggerMock.Object);
        using var driverRemoteConnection =
            new DriverRemoteConnection(TestHost, TestPort, loggerFactory: loggerFactoryMock.Object);
        var bytecodeToLog = SomeValidBytecode;

        await driverRemoteConnection.SubmitAsync<object, object>(bytecodeToLog);
        
        loggerMock.VerifyMessageWasLogged(LogLevel.Debug, bytecodeToLog.ToString());
    }
    
    [Fact]
    public async Task ShouldNotLogForDisabledLogLevel()
    {
        var loggerFactoryMock = new Mock<ILoggerFactory>();
        var loggerMock = new Mock<ILogger>();
        loggerMock.Setup(m => m.IsEnabled(It.IsAny<LogLevel>())).Returns(false);
        loggerFactoryMock.Setup(m => m.CreateLogger(It.IsAny<string>())).Returns(loggerMock.Object);
        using var driverRemoteConnection =
            new DriverRemoteConnection(TestHost, TestPort, loggerFactory: loggerFactoryMock.Object);

        await driverRemoteConnection.SubmitAsync<object, object>(SomeValidBytecode);
        
        loggerMock.VerifyNothingWasLogged();
    }
            
    [Fact]
    public async Task ShouldLogWithLoggerFactoryFromGremlinClient()
    {
        var loggerFactoryMock = new Mock<ILoggerFactory>();
        var loggerMock = new Mock<ILogger>();
        loggerMock.Setup(m => m.IsEnabled(It.IsAny<LogLevel>())).Returns(true);
        loggerFactoryMock.Setup(m => m.CreateLogger(It.IsAny<string>())).Returns(loggerMock.Object);
        using var gremlinClient =
            new GremlinClient(new GremlinServer(TestHost, TestPort), loggerFactory: loggerFactoryMock.Object);
        var driverRemoteConnection = new DriverRemoteConnection(gremlinClient);
        var bytecodeToLog = SomeValidBytecode;

        await driverRemoteConnection.SubmitAsync<object, object>(SomeValidBytecode);

        loggerMock.VerifyMessageWasLogged(LogLevel.Debug, bytecodeToLog.ToString());
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