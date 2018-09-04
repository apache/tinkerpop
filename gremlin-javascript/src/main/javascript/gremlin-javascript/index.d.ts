/**
  * ./lib/process/traversal
  */
namespace t {
  class EnumValue {
    constructor(typeName: string, elementName: string);
    typeName: string;
    elementName: string;
    /**
      * Returns elementName
      */
    toString(): string;
  }

  /**
    * Represents an operation.
    */
  class P {
    constructor(operator: string, value: string, other: string);
    operator: string;
    value: string;
    other: string;
    /**
      * Returns the string representation of the instance.
      * @returns {string}
      */
    toString(): string;
    and(arg: string): string;
    or(arg: string): string;
    static between(...args): P;
    static eq(...args): P;
    static gt(...args): P;
    static gte(...args): P;
    static inside(...args): P;
    static lt(...args): P;
    static lte(...args): P;
    static neq(...args): P;
    static not(...args): P;
    static outside(...args): P;
    static test(...args): P;
    static within(...args): P;
    static without(...args): P;
  }

  class Traversal {
    constructor(graph: graph.Graph, traversalStrategies: strategiesModule.TraversalStrategies, bytecode: Bytecode);
    graph: graph.Graph;
    traversalStrategies: strategiesModule.TraversalStrategies;
    bytecode: Bytecode;
    traversers: Traverser[];
    sideEffects: TraversalSideEffects;

    getBytecode(): Bytecode;

    /**
      * Returns an Array containing the traverser objects.
      * @returns {Promise.<Array>}
      */
    toList(): Promise<any[]>;

    /**
      * Iterates all Traverser instances in the traversal.
      * @returns {Promise}
      */
    iterate(): Promise<any>;

    /**
      * Async iterator method implementation.
      * Returns a promise containing an iterator item.
      * @returns {Promise.<{value, done}>}
      */
    next(): Promise<{ value: any; done: boolean }>;

    /**
      * Returns the Bytecode JSON representation of the traversal
      * @returns {String}
      */
    toString(): string;
  }

  class Traverser {
    constructor(object: any, bulk: number);
    object: any;
    bulk: number;
  }

  /**
    * Return type of `function toEnum(typeName, keys)`
    */
  interface EnumInstance {
    [jsKey: string]: EnumValue;
  }
}

/**
  * ./lib/process/graph-traversal
  */
namespace gt {
  /**
    * Represents a graph traversal.
    */
  class GraphTraversal extends t.Traversal {
    /**
      * Graph traversal V method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    V(...args): GraphTraversal;

    /**
      * Graph traversal addE method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    addE(...args): GraphTraversal;

    /**
      * Graph traversal addInE method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    addInE(...args): GraphTraversal;

    /**
      * Graph traversal addOutE method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    addOutE(...args): GraphTraversal;

    /**
      * Graph traversal addV method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    addV(...args): GraphTraversal;

    /**
      * Graph traversal aggregate method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    aggregate(...args): GraphTraversal;

    /**
      * Graph traversal and method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    and(...args): GraphTraversal;

    /**
      * Graph traversal as method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    as(...args): GraphTraversal;

    /**
      * Graph traversal barrier method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    barrier(...args): GraphTraversal;

    /**
      * Graph traversal both method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    both(...args): GraphTraversal;

    /**
      * Graph traversal bothE method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    bothE(...args): GraphTraversal;

    /**
      * Graph traversal bothV method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    bothV(...args): GraphTraversal;

    /**
      * Graph traversal branch method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    branch(...args): GraphTraversal;

    /**
      * Graph traversal by method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    by(...args): GraphTraversal;

    /**
      * Graph traversal cap method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    cap(...args): GraphTraversal;

    /**
      * Graph traversal choose method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    choose(...args): GraphTraversal;

    /**
      * Graph traversal coalesce method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    coalesce(...args): GraphTraversal;

    /**
      * Graph traversal coin method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    coin(...args): GraphTraversal;

    /**
      * Graph traversal constant method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    constant(...args): GraphTraversal;

    /**
      * Graph traversal count method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    count(...args): GraphTraversal;

    /**
      * Graph traversal cyclicPath method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    cyclicPath(...args): GraphTraversal;

    /**
      * Graph traversal dedup method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    dedup(...args): GraphTraversal;

    /**
      * Graph traversal drop method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    drop(...args): GraphTraversal;

    /**
      * Graph traversal emit method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    emit(...args): GraphTraversal;

    /**
      * Graph traversal filter method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    filter(...args): GraphTraversal;

    /**
      * Graph traversal flatMap method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    flatMap(...args): GraphTraversal;

    /**
      * Graph traversal fold method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    fold(...args): GraphTraversal;

    /**
      * Graph traversal from method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    from_(...args): GraphTraversal;

    /**
      * Graph traversal group method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    group(...args): GraphTraversal;

    /**
      * Graph traversal groupCount method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    groupCount(...args): GraphTraversal;

    /**
      * Graph traversal groupV3d0 method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    groupV3d0(...args): GraphTraversal;

    /**
      * Graph traversal has method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    has(...args): GraphTraversal;

    /**
      * Graph traversal hasId method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    hasId(...args): GraphTraversal;

    /**
      * Graph traversal hasKey method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    hasKey(...args): GraphTraversal;

    /**
      * Graph traversal hasLabel method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    hasLabel(...args): GraphTraversal;

    /**
      * Graph traversal hasNot method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    hasNot(...args): GraphTraversal;

    /**
      * Graph traversal hasValue method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    hasValue(...args): GraphTraversal;

    /**
      * Graph traversal id method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    id(...args): GraphTraversal;

    /**
      * Graph traversal identity method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    identity(...args): GraphTraversal;

    /**
      * Graph traversal in method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    in_(...args): GraphTraversal;

    /**
      * Graph traversal inE method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    inE(...args): GraphTraversal;

    /**
      * Graph traversal inV method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    inV(...args): GraphTraversal;

    /**
      * Graph traversal inject method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    inject(...args): GraphTraversal;

    /**
      * Graph traversal is method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    is(...args): GraphTraversal;

    /**
      * Graph traversal key method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    key(...args): GraphTraversal;

    /**
      * Graph traversal label method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    label(...args): GraphTraversal;

    /**
      * Graph traversal limit method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    limit(...args): GraphTraversal;

    /**
      * Graph traversal local method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    local(...args): GraphTraversal;

    /**
      * Graph traversal loops method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    loops(...args): GraphTraversal;

    /**
      * Graph traversal map method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    map(...args): GraphTraversal;

    /**
      * Graph traversal mapKeys method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    mapKeys(...args): GraphTraversal;

    /**
      * Graph traversal mapValues method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    mapValues(...args): GraphTraversal;

    /**
      * Graph traversal match method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    match(...args): GraphTraversal;

    /**
      * Graph traversal max method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    max(...args): GraphTraversal;

    /**
      * Graph traversal mean method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    mean(...args): GraphTraversal;

    /**
      * Graph traversal min method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    min(...args): GraphTraversal;

    /**
      * Graph traversal not method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    not(...args): GraphTraversal;

    /**
      * Graph traversal option method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    option(...args): GraphTraversal;

    /**
      * Graph traversal optional method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    optional(...args): GraphTraversal;

    /**
      * Graph traversal or method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    or(...args): GraphTraversal;

    /**
      * Graph traversal order method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    order(...args): GraphTraversal;

    /**
      * Graph traversal otherV method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    otherV(...args): GraphTraversal;

    /**
      * Graph traversal out method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    out(...args): GraphTraversal;

    /**
      * Graph traversal outE method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    outE(...args): GraphTraversal;

    /**
      * Graph traversal outV method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    outV(...args): GraphTraversal;

    /**
      * Graph traversal pageRank method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    pageRank(...args): GraphTraversal;

    /**
      * Graph traversal path method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    path(...args): GraphTraversal;

    /**
      * Graph traversal peerPressure method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    peerPressure(...args): GraphTraversal;

    /**
      * Graph traversal profile method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    profile(...args): GraphTraversal;

    /**
      * Graph traversal program method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    program(...args): GraphTraversal;

    /**
      * Graph traversal project method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    project(...args): GraphTraversal;

    /**
      * Graph traversal properties method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    properties(...args): GraphTraversal;

    /**
      * Graph traversal property method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    property(...args): GraphTraversal;

    /**
      * Graph traversal propertyMap method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    propertyMap(...args): GraphTraversal;

    /**
      * Graph traversal range method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    range(...args): GraphTraversal;

    /**
      * Graph traversal repeat method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    repeat(...args): GraphTraversal;

    /**
      * Graph traversal sack method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    sack(...args): GraphTraversal;

    /**
      * Graph traversal sample method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    sample(...args): GraphTraversal;

    /**
      * Graph traversal select method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    select(...args): GraphTraversal;

    /**
      * Graph traversal sideEffect method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    sideEffect(...args): GraphTraversal;

    /**
      * Graph traversal simplePath method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    simplePath(...args): GraphTraversal;

    /**
      * Graph traversal store method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    store(...args): GraphTraversal;

    /**
      * Graph traversal subgraph method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    subgraph(...args): GraphTraversal;

    /**
      * Graph traversal sum method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    sum(...args): GraphTraversal;

    /**
      * Graph traversal tail method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    tail(...args): GraphTraversal;

    /**
      * Graph traversal timeLimit method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    timeLimit(...args): GraphTraversal;

    /**
      * Graph traversal times method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    times(...args): GraphTraversal;

    /**
      * Graph traversal to method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    to(...args): GraphTraversal;

    /**
      * Graph traversal toE method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    toE(...args): GraphTraversal;

    /**
      * Graph traversal toV method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    toV(...args): GraphTraversal;

    /**
      * Graph traversal tree method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    tree(...args): GraphTraversal;

    /**
      * Graph traversal unfold method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    unfold(...args): GraphTraversal;

    /**
      * Graph traversal union method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    union(...args): GraphTraversal;

    /**
      * Graph traversal until method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    until(...args): GraphTraversal;

    /**
      * Graph traversal value method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    value(...args): GraphTraversal;

    /**
      * Graph traversal valueMap method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    valueMap(...args): GraphTraversal;

    /**
      * Graph traversal values method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    values(...args): GraphTraversal;

    /**
      * Graph traversal where method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    where(...args): GraphTraversal;
  }

  /**
    * Represents the primary DSL of the Gremlin traversal machine.
    */
  class GraphTraversalSource {
    constructor(graph: graph.Graph, traversalStrategies: strategiesModule.TraversalStrategies, bytecode: Bytecode);

    withRemote(remoteConnection: rc.RemoteConnection): GraphTraversalSource;

    /**
      * Returns the string representation of the GraphTraversalSource.
      */
    toString(): string;

    /**
      * Graph Traversal Source withBulk method.
      * @param {...Object} args
      * @returns {GraphTraversalSource}
      */
    withBulk(...args): GraphTraversalSource;

    /**
      * Graph Traversal Source withPath method.
      * @param {...Object} args
      * @returns {GraphTraversalSource}
      */
    withPath(...args): GraphTraversalSource;

    /**
      * Graph Traversal Source withSack method.
      * @param {...Object} args
      * @returns {GraphTraversalSource}
      */
    withSack(...args): GraphTraversalSource;

    /**
      * Graph Traversal Source withSideEffect method.
      * @param {...Object} args
      * @returns {GraphTraversalSource}
      */
    withSideEffect(...args): GraphTraversalSource;

    /**
      * Graph Traversal Source withStrategies method.
      * @param {...Object} args
      * @returns {GraphTraversalSource}
      */
    withStrategies(...args): GraphTraversalSource;

    /**
      * Graph Traversal Source withoutStrategies method.
      * @param {...Object} args
      * @returns {GraphTraversalSource}
      */
    withoutStrategies(...args): GraphTraversalSource;

    /**
      * E GraphTraversalSource step method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    E(...args): GraphTraversal;

    /**
      * V GraphTraversalSource step method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    V(...args): GraphTraversal;

    /**
      * addV GraphTraversalSource step method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    addV(...args): GraphTraversal;

    /**
      * inject GraphTraversalSource step method.
      * @param {...Object} args
      * @returns {GraphTraversal}
      */
    inject(...args): GraphTraversal;
  }

  interface GraphTraversalStatics {
    V(...args): GraphTraversal;
    addE(...args): GraphTraversal;
    addInE(...args): GraphTraversal;
    addOutE(...args): GraphTraversal;
    addV(...args): GraphTraversal;
    aggregate(...args): GraphTraversal;
    and(...args): GraphTraversal;
    as(...args): GraphTraversal;
    barrier(...args): GraphTraversal;
    both(...args): GraphTraversal;
    bothE(...args): GraphTraversal;
    bothV(...args): GraphTraversal;
    branch(...args): GraphTraversal;
    cap(...args): GraphTraversal;
    choose(...args): GraphTraversal;
    coalesce(...args): GraphTraversal;
    coin(...args): GraphTraversal;
    constant(...args): GraphTraversal;
    count(...args): GraphTraversal;
    cyclicPath(...args): GraphTraversal;
    dedup(...args): GraphTraversal;
    drop(...args): GraphTraversal;
    emit(...args): GraphTraversal;
    filter(...args): GraphTraversal;
    flatMap(...args): GraphTraversal;
    fold(...args): GraphTraversal;
    group(...args): GraphTraversal;
    groupCount(...args): GraphTraversal;
    groupV3d0(...args): GraphTraversal;
    has(...args): GraphTraversal;
    hasId(...args): GraphTraversal;
    hasKey(...args): GraphTraversal;
    hasLabel(...args): GraphTraversal;
    hasNot(...args): GraphTraversal;
    hasValue(...args): GraphTraversal;
    id(...args): GraphTraversal;
    identity(...args): GraphTraversal;
    in_(...args): GraphTraversal;
    inE(...args): GraphTraversal;
    inV(...args): GraphTraversal;
    inject(...args): GraphTraversal;
    is(...args): GraphTraversal;
    key(...args): GraphTraversal;
    label(...args): GraphTraversal;
    limit(...args): GraphTraversal;
    local(...args): GraphTraversal;
    loops(...args): GraphTraversal;
    map(...args): GraphTraversal;
    mapKeys(...args): GraphTraversal;
    mapValues(...args): GraphTraversal;
    match(...args): GraphTraversal;
    max(...args): GraphTraversal;
    mean(...args): GraphTraversal;
    min(...args): GraphTraversal;
    not(...args): GraphTraversal;
    optional(...args): GraphTraversal;
    or(...args): GraphTraversal;
    order(...args): GraphTraversal;
    otherV(...args): GraphTraversal;
    out(...args): GraphTraversal;
    outE(...args): GraphTraversal;
    outV(...args): GraphTraversal;
    path(...args): GraphTraversal;
    project(...args): GraphTraversal;
    properties(...args): GraphTraversal;
    property(...args): GraphTraversal;
    propertyMap(...args): GraphTraversal;
    range(...args): GraphTraversal;
    repeat(...args): GraphTraversal;
    sack(...args): GraphTraversal;
    sample(...args): GraphTraversal;
    select(...args): GraphTraversal;
    sideEffect(...args): GraphTraversal;
    simplePath(...args): GraphTraversal;
    store(...args): GraphTraversal;
    subgraph(...args): GraphTraversal;
    sum(...args): GraphTraversal;
    tail(...args): GraphTraversal;
    timeLimit(...args): GraphTraversal;
    times(...args): GraphTraversal;
    to(...args): GraphTraversal;
    toE(...args): GraphTraversal;
    toV(...args): GraphTraversal;
    tree(...args): GraphTraversal;
    unfold(...args): GraphTraversal;
    union(...args): GraphTraversal;
    until(...args): GraphTraversal;
    value(...args): GraphTraversal;
    valueMap(...args): GraphTraversal;
    values(...args): GraphTraversal;
    where(...args): GraphTraversal;
  }
}

/**
  * ./lib/process/traversal-strategy
  */
namespace strategiesModule {
  class TraversalStrategies {
    /**
      * Creates a new instance of TraversalStrategies.
      * @param {TraversalStrategies} [parent] The parent strategies from where to clone the values from.
      * @constructor
      */
    constructor(parent?: TraversalStrategies);
    strategies: TraversalStrategy[];
    addStrategy(strategy: TraversalStrategy): void;
    applyStrategies(traversal: t.Traversal): Promise<any>;
  }
  abstract class TraversalStrategy {
    apply(traversal: t.Traversal): Promise<any>;
  }
}

/**
  * ./lib/structure/graph
  */
namespace graph {
  class Graph {
    /**
      * Returns the graph traversal source.
      * @returns {GraphTraversalSource}
      */
    traversal(): gt.GraphTraversalSource;
    toString(): string;
  }

  class Element {
    constructor(id: string, label: string);
    id: string;
    label: string;
    /**
      * Compares this instance to another and determines if they can be considered as equal.
      * @param {Element} other
      * @returns {boolean}
      */
    equals(other: Element): boolean;
  }

  class Vertex extends Element {
    constructor(id: string, label: string, properties: Properties);
    properties: Properties;
    toString(): string;
  }

  class Edge extends Element {
    constructor(id: string, outV: Vertex, label: string, inV: Vertex, properties?: Properties);
    outV: Vertex;
    inV: Vertex;
    properties: Properties;
    toString(): string;
  }

  class VertexProperty extends Element {
    constructor(id: string, label: string, value: string, properties: { [key: string]: VertexProperty });
    value: string;
    key: string;
    properties: Properties;
  }

  class Property {
    constructor(key: string, value: string);
    key: string;
    value: string;
    toString(): string;
    equals(other: Property): boolean;
  }

  class Properties {
    [key: string]: string;
  }

  class Path {
    constructor(labels: string[], objects: any[]);
    labels: string[];
    objects: any[];
    equals(other: Path): boolean;
  }
}

/**
  * ./lib/structure/io/graph-serializer
  */
namespace gs {
  class TypeSerializer {
    serialize();
    deserialize();
    canBeUsedFor();
  }

  interface GraphSONOptions {
    serializers?: {
      [graphSON2Type: string]: TypeSerializer;
    };
  }

  /**
    * GraphSON Writer
    */
  class GraphSONWriter {
    /**
      * @param {Object} [options]
      * @param {Object} options.serializers An object used as an associative array with GraphSON 2 type name as keys and
      * serializer instances as values, ie: { 'g:Int64': longSerializer }.
      * @constructor
      */
    constructor(options?: GraphSONOptions);
    adaptObject(value: any);

    /**
      * Returns the GraphSON representation of the provided object instance.
      */
    write(obj: any): string;
  }

  class GraphSONReader {
    /**
      * GraphSON Reader
      * @param {Object} [options]
      * @param {Object} [options.serializers] An object used as an associative array with GraphSON 2 type name as keys and
      * deserializer instances as values, ie: { 'g:Int64': longSerializer }.
      * @constructor
      */
    constructor(options?: GraphSONOptions);
    read(obj: any): any;
  }
}

/**
  * ./lib/driver/remote-connection
  */
namespace rc {
  class RemoteConnection {
    constructor(url: string);
    url: string;
    submit(bytecode: Bytecode): Promise<any>;
  }

  class RemoteStrategy extends strategiesModule.TraversalStrategy {
    constructor(connection: RemoteConnection);
    connection: RemoteConnection;
  }

  class RemoteTraversal extends t.Traversal {
    constructor(traversers: t.Traverser[], sideEffects: TraversalSideEffects);
  }
}

class Bytecode {
  constructor(toClone?: Bytecode);
  addSource(name: string, values: any[]): Bytecode;
  addStep(name: string, values: any[]): Bytecode;
  toString(val: t.EnumValue): string;
}

interface DriverRemoteConnectionOptions {
  /**
    * Trusted certificates.
    */
  ca?: any[];

  /**
    * The certificate key.
    */
  cert?: string | any[] | Buffer;

  /**
    * The mime type to use.
    */
  mimeType?: string;

  /**
    * The private key, certificate, and CA certs.
    */
  pfx?: string | Buffer;

  /**
    * The reader to use
    */
  reader?: gs.GraphSONReader;

  /**
    * Determines whether to verify or not the server certificate.
    */
  rejectUnauthorized?: boolean;

  /**
    * The traversal source. Defaults to: 'g'.
    */
  traversalSource?: string;

  /**
    * The writer to use.
    */
  writer?: gs.GraphSONWriter;
}

class DriverRemoteConnection extends rc.RemoteConnection {
  /**
    * Creates a new instance of DriverRemoteConnection.
    * @param url The resource uri.
    * @param options The connection options.
    */
  constructor(url: string, options?: DriverRemoteConnectionOptions);

  /**
    * Opens the connection, if its not already opened.
    * @returns {Promise}
    */
  open(): Promise<any>;

  /**
    * Closes the Connection.
    * @return {Promise}
    */
  close(): Promise<any>;
}

class TraversalSideEffects {}

class Long {
  constructor(value: string | number);
  value: string;
}

const gremlin: {
  driver: {
    RemoteConnection: typeof rc.RemoteConnection;
    RemoteStrategy: typeof rc.RemoteStrategy;
    RemoteTraversal: typeof rc.RemoteTraversal;
    DriverRemoteConnection: typeof DriverRemoteConnection;
  };
  process: {
    Bytecode: typeof Bytecode;
    EnumValue: typeof t.EnumValue;
    P: typeof t.P;
    Traversal: typeof t.Traversal;
    TraversalSideEffects: typeof TraversalSideEffects;
    TraversalStrategies: typeof strategiesModule.TraversalStrategies;
    TraversalStrategy: typeof strategiesModule.TraversalStrategy;
    Traverser: typeof t.Traverser;
    barrier: t.EnumInstance;
    cardinality: t.EnumInstance;
    column: t.EnumInstance;
    direction: t.EnumInstance;
    operator: t.EnumInstance;
    order: t.EnumInstance;
    pop: t.EnumInstance;
    scope: t.EnumInstance;
    t: t.EnumInstance;
    GraphTraversal: typeof gt.GraphTraversal;
    GraphTraversalSource: typeof gt.GraphTraversalSource;
    statics: gt.GraphTraversalStatics;
  };
  structure: {
    io: {
      GraphSONReader: typeof gs.GraphSONReader;
      GraphSONWriter: typeof gs.GraphSONWriter;
    };
    Edge: typeof graph.Edge;
    Graph: typeof graph.Graph;
    Path: typeof graph.Path;
    Property: typeof graph.Property;
    Vertex: typeof graph.Vertex;
    VertexProperty: typeof graph.VertexProperty;
    toLong(value: string | number): Long;
  };
};
export = gremlin;
