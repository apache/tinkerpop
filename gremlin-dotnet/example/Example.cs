using Gremlin;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.Structure.IO.GraphSON;

// Common imports
using static Gremlin.Net.Process.Traversal.AnonymousTraversalSource;
using static Gremlin.Net.Process.Traversal.__;
using static Gremlin.Net.Process.Traversal.P;
using static Gremlin.Net.Process.Traversal.Order;
using static Gremlin.Net.Process.Traversal.Operator;
using static Gremlin.Net.Process.Traversal.Pop;
using static Gremlin.Net.Process.Traversal.Scope;
using static Gremlin.Net.Process.Traversal.TextP;
using static Gremlin.Net.Process.Traversal.Column;
using static Gremlin.Net.Process.Traversal.Direction;
using static Gremlin.Net.Process.Traversal.Cardinality;
using static Gremlin.Net.Process.Traversal.CardinalityValue;
using static Gremlin.Net.Process.Traversal.T;

var server = new GremlinServer("localhost", 8182);

// Connecting to the server
// using var remoteConnection = new DriverRemoteConnection(new GremlinClient(server), "g");

// Connecting to the server with customized configurations
using var remoteConnection = new DriverRemoteConnection(new GremlinClient(
    new GremlinServer(hostname:"localhost", port:8182, enableSsl:false, username:"", password:"")), "g");

// Connecting and specifying a serializer
var client = new GremlinClient(server, new GraphSON3MessageSerializer());

// Creating the graph traversal
var g = Traversal().WithRemote(remoteConnection);
g.AddV("test").Next();
Console.WriteLine(g);
