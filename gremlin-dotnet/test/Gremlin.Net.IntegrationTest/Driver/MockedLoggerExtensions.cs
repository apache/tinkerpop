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
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Gremlin.Net.IntegrationTest.Driver;

public static class MockedLoggerExtensions
{
    public static void VerifyMessageWasLogged(this ILogger logger, LogLevel expectedLogLevel, string logMessagePart)
    {
        logger.Received().Log(expectedLogLevel, Arg.Any<EventId>(),
            Arg.Is<Arg.AnyType>((object a) => a.ToString()!.Contains(logMessagePart)), null,
            Arg.Any<Func<Arg.AnyType, Exception?, string>>());
    }

    public static void VerifyNothingWasLogged(this ILogger logger)
    {
        logger.DidNotReceive().Log(Arg.Any<LogLevel>(), Arg.Any<EventId>(), Arg.Any<Arg.AnyType>(),
            Arg.Any<Exception>(), Arg.Any<Func<Arg.AnyType, Exception?, string>>());
    }
}