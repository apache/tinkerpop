using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.Structure.IO.GraphSON;
using static Gremlin.Net.Process.Traversal.AnonymousTraversalSource;

public class ConnectionExample
{
    static void Main()
    {
        withRemote();
        withConf();
        withSerializer();
    }

    // Connecting to the server
    static void withRemote()
    {
        var server = new GremlinServer("localhost", 8182);
        using var remoteConnection = new DriverRemoteConnection(new GremlinClient(server), "g");
        var g = Traversal().WithRemote(remoteConnection);
    }

    // Connecting to the server with customized configurations
    static void withConf()
    {
        using var remoteConnection = new DriverRemoteConnection(new GremlinClient(
        new GremlinServer(hostname: "localhost", port: 8182, enableSsl: false, username: "", password: "")), "g");
        var g = Traversal().WithRemote(remoteConnection);
    }

    // Specifying a serializer
    static void withSerializer()
    {
        var server = new GremlinServer("localhost", 8182);
        var client = new GremlinClient(server, new GraphSON3MessageSerializer());
        using var remoteConnection = new DriverRemoteConnection(client, "g");
        var g = Traversal().WithRemote(remoteConnection);
    }
}
