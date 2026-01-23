# Remote Transaction Flow

## Overview

This diagram shows the complete client-server interaction for remote transactions over HTTP. Transactions are thread-bound: when `tx.begin()` is called, all subsequent traversals on that thread automatically participate in the transaction.

Making these remote transactions thread-bound on the GLV-side makes it match the behavior for embedded transactions which are threaded by default. The API will likely force transactions to disallow multithreaded transactions which matches traditional relation database behavior. However, a subsequent update will likely allow multiple of these thread-bound transactions to exist, that is, a single thread will be able to have one or more transactions.

## Sequence Diagram

```mermaid
sequenceDiagram
    participant Client as Client (Thread 1)
    participant GTS as GraphTraversalSource
    participant Conn as DriverRemoteConnection
    participant Server as Gremlin Server

    Note over Client,Server: Setup
    Client->>Conn: traversal().with(connection)
    Conn-->>Client: GraphTraversalSource (g)

    Note over Client,Server: Begin Transaction
    Client->>GTS: g.tx()
    GTS-->>Client: Transaction (tx)
    Client->>Conn: tx.begin()
    Conn->>Server: POST /gremlin/tx
    Server-->>Conn: 201 Created<br/>{txId: "abc-123"}
    Note over Conn: Store txId in ThreadLocal
    Conn-->>Client: void

    Note over Client,Server: Execute Traversals (Thread-Bound)
    Client->>GTS: g.addV("person")
    GTS->>Conn: submit(traversal)
    Note over Conn: Check ThreadLocal: txId = "abc-123"
    Conn->>Server: POST /gremlin/tx/abc-123
    Server-->>Conn: 200 OK
    Conn-->>GTS: Result
    GTS-->>Client: Traversal

    Client->>GTS: g.V().has("name", "x")
    GTS->>Conn: submit(traversal)
    Note over Conn: Check ThreadLocal: txId = "abc-123"
    Conn->>Server: POST /gremlin/tx/abc-123
    Server-->>Conn: 200 OK
    Conn-->>GTS: Result
    GTS-->>Client: Traversal

    Note over Client,Server: Commit Transaction
    Client->>Conn: tx.commit()
    Conn->>Server: POST /gremlin/tx/abc-123/commit
    Server-->>Conn: 200 OK<br/>{status: "COMMITTED"}
    Note over Conn: Clear ThreadLocal
    Conn-->>Client: void
```

## Rollback Flow

```mermaid
sequenceDiagram
    participant Client
    participant Conn as DriverRemoteConnection
    participant Server as Gremlin Server

    Note over Client,Server: Transaction is Active
    Client->>Conn: tx.rollback()
    Conn->>Server: POST /gremlin/tx/abc-123/rollback
    Server-->>Conn: 200 OK<br/>{status: "ROLLED_BACK"}
    Note over Conn: Clear ThreadLocal
    Conn-->>Client: void
```

## Thread-Bound Behavior

### Key Points

1. **ThreadLocal Storage**: When `tx.begin()` is called, the transaction ID is stored in a ThreadLocal variable
2. **Automatic Binding**: All traversals submitted on that thread automatically check ThreadLocal for an active transaction
3. **Endpoint Selection**: 
   - If transaction ID found in ThreadLocal → use `POST /gremlin/tx/{txId}`
   - If no transaction ID → use `POST /gremlin` (non-transactional)
4. **Thread Cleanup**: `tx.commit()` or `tx.rollback()` clears the ThreadLocal for that thread
5. **Thread Isolation**: Different threads can have different active transactions

### Behavior Diagram

```mermaid
flowchart TD
    A[Client calls tx.begin] --> B[POST /gremlin/tx]
    B --> C[Server returns txId]
    C --> D[Store txId in ThreadLocal]
    D --> E[Client executes g.addV...]
    E --> F{Check ThreadLocal}
    F -->|txId found| G[POST /gremlin/tx/txId]
    F -->|No txId| H[POST /gremlin]
    G --> I[Execute in transaction]
    H --> J[Execute non-transactional]
    I --> K[Client calls tx.commit]
    K --> L[POST /gremlin/tx/txId/commit]
    L --> M[Clear ThreadLocal]
```
