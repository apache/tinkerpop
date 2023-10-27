using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Remote;
using static Gremlin.Net.Process.Traversal.AnonymousTraversalSource;

public class BasicGremlinExample
{
    static async Task Main()
    {
        var server = new GremlinServer("localhost", 8182);
        using var remoteConnection = new DriverRemoteConnection(new GremlinClient(server), "g");
        var g = Traversal().WithRemote(remoteConnection);

        // Basic Gremlin: adding and retrieving data
        var v1 = g.AddV("person").Property("name", "marko").Next();
        var v2 = g.AddV("person").Property("name", "stephen").Next();
        var v3 = g.AddV("person").Property("name", "vadas").Next();

        // Be sure to use a terminating step like next() or iterate() so that the traversal "executes"
        // Iterate() does not return any data and is used to just generate side-effects (i.e. write data to the database)
        g.V(v1).AddE("knows").To(v2).Property("weight", 0.75).Iterate();
        g.V(v1).AddE("knows").To(v3).Property("weight", 0.75).Iterate();

        // Retrieve the data from the "marko" vertex
        var marko = await g.V().Has("person", "name", "marko").Values<string>("name").Promise(t => t.Next());
        Console.WriteLine("name: " + marko);

        // Find the "marko" vertex and then traverse to the people he "knows" and return their data
        var peopleMarkoKnows = await g.V().Has("person", "name", "marko").Out("knows").Values<string>("name").Promise(t => t.ToList());
        foreach (var person in peopleMarkoKnows)
        {
            Console.WriteLine("marko knows " + person);
        }
    }
}

