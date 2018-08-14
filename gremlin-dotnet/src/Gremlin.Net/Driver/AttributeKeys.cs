using System;
using System.Collections.Generic;
using System.Text;

namespace Gremlin.Net.Driver
{
    internal static class AttributeKeys
    {
        public const string CosmosDbRetryAfterMs = "x-ms-retry-after-ms";
        public const string CosmosDbStatusCode = "x-ms-status-code";
        public const string CosmosDbRequestRateException = "RequestRateTooLargeException";
    }
}
