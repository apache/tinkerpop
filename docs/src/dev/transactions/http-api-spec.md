## Request Format Specification

**Endpoint:** `POST /gremlin`  
**Content-Type:** `application/json`

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `gremlin` | String | The Gremlin query to execute. For tx control: `"g.tx().begin()"`, `"g.tx().commit()"`, `"g.tx().rollback()"` |
| `g` | String | Graph/traversal source alias (e.g., `"g"`, `"gmodern"`, `"gcrew"`) |
| `transactionId` | String | Client-generated UUID. Must be consistent across all requests in the same transaction |

### Required Header

| Header | Type | Description |
|--------|------|-------------|
| `X-Transaction-Id` | String | Same value as body `transactionId`. Used for load balancer routing |

---

## Transaction Control Commands

The server must detect these exact Gremlin strings to handle transaction lifecycle:

```mermaid
flowchart LR
    subgraph detection["Transaction Control Detection"]
        begin["g.tx#40;#41;.begin#40;#41;"]
        commit["g.tx#40;#41;.commit#40;#41;"]
        rollback["g.tx#40;#41;.rollback#40;#41;"]
        other["Any other gremlin"]
    end

    begin --> beginAction["Create new TransactionContext"]
    commit --> commitAction["Commit and cleanup"]
    rollback --> rollbackAction["Rollback and cleanup"]
    other --> otherAction["Execute within transaction"]
```

**IMPORTANT:** String matching should be exact (after trimming whitespace)

---


## Protocol Flow

```mermaid
sequenceDiagram
    participant Client
    participant Server

    Note over Client,Server: 1. BEGIN TRANSACTION

    Client->>Server: POST /gremlin<br/>Header: X-Transaction-Id: abc-123-def-456<br/>Body: {"gremlin": "g.tx().begin()", "g": "gmodern", "transactionId": "abc-123-def-456"}
    
    Note right of Server: Create TransactionContext<br/>• Allocate thread for tx<br/>• Open graph transaction<br/>• Start timeout timer
    
    Server-->>Client: 200 OK

    Note over Client,Server: 2. EXECUTE OPERATIONS (repeat as needed)

    Client->>Server: POST /gremlin<br/>Header: X-Transaction-Id: abc-123-def-456<br/>Body: {"gremlin": "g.addV('person').property('name','josh')", "g": "gmodern", "transactionId": "abc-123-def-456"}
    
    Note right of Server: Lookup TransactionContext<br/>Execute on tx thread<br/>Reset timeout timer
    
    Server-->>Client: 200 OK + results

    Note over Client,Server: 3. COMMIT or ROLLBACK

    Client->>Server: POST /gremlin<br/>Header: X-Transaction-Id: abc-123-def-456<br/>Body: {"gremlin": "g.tx().commit()", "g": "gmodern", "transactionId": "abc-123-def-456"}
    
    Note right of Server: Commit graph transaction<br/>Cleanup TransactionContext<br/>Release thread
    
    Server-->>Client: 200 OK
```

---

## Transaction ID: Dual Transmission Design

### Why Transaction ID Appears Twice

```mermaid
flowchart TB
    subgraph request["HTTP Request Structure"]
        subgraph headers["HTTP HEADERS"]
            h1["Content-Type: application/json"]
            h2["X-Transaction-Id: abc-123-def-456"]
        end
        subgraph body["HTTP BODY #40;JSON#41;"]
            b1["gremlin: g.addV#40;person#41;"]
            b2["g: gmodern"]
            b3["transactionId: abc-123-def-456"]
        end
    end

    h2 -->|FOR LOAD BALANCER ROUTING| lb["Load Balancers, Proxies, API Gateways"]
    b3 -->|FOR SERVER PROCESSING| server["Graph Database Server"]
```

### Architectural Rationale

| Aspect | X-Transaction-Id HEADER | transactionId BODY FIELD |
|--------|------------------------|--------------------------|
| **Purpose** | Infrastructure routing | Application logic |
| **Used by** | Load balancers, proxies, API gateways | Graph database server to lookup transaction state |
| **Why needed** | Load balancers typically only inspect headers, not body. Enables sticky sessions without application-layer parsing. Works with any LB that supports header-based routing. | Server needs to associate request with correct transaction. Part of the Gremlin request protocol specification. Consistent with other request fields (gremlin, g, bindings). |

### Load Balancer Sticky Routing

```mermaid
flowchart TB
    LB["Load Balancer<br/>(reads X-Transaction-Id header)"]
    
    LB --> ServerA["Server A<br/>tx: abc, def"]
    LB --> ServerB["Server B<br/>tx: xyz, uvw"]
    LB --> ServerC["Server C<br/>tx: 123, 456"]
    
    req1["Request with<br/>X-Transaction-Id: abc"] -.->|Always routes to| ServerA
    req2["Request with<br/>X-Transaction-Id: xyz"] -.->|Always routes to| ServerB
```