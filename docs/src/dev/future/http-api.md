## Request Format Specification

**Endpoint:** `POST /gremlin`  
**Content-Type:** `application/json`

### Begin Request (no transactionId)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `gremlin` | String | Yes | Must be `"g.tx().begin()"` |
| `g` | String | No | Graph/traversal source alias (e.g., `"g"`, `"gmodern"`, `"gcrew"`). Defaults to `"g"` if not specified |

### Begin Response

| Field | Type | Description |
|-------|------|-------------|
| `transactionId` | String | Server-generated String (UUID recommended) for this transaction  |
| `status` | Object | Standard status object with code and message |

### Subsequent Requests (with transactionId)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `gremlin` | String | Yes | The Gremlin query to execute. For tx control: `"g.tx().commit()"`, `"g.tx().rollback()"` |
| `g` | String | No | Graph/traversal source alias (e.g., `"g"`, `"gmodern"`, `"gcrew"`). Defaults to `"g"` if not specified |
| `transactionId` | String | Yes | The server-generated transaction ID from the begin response. Omit for non-transactional requests |

### Required Header (for requests after begin)

| Header | Type | Description |
|--------|------|-------------|
| `X-Transaction-Id` | String | Same value as body `transactionId`. Used for load balancer routing |

---

## Protocol Flow

```mermaid
sequenceDiagram
    participant Client
    participant Server

    Note over Client,Server: 1. BEGIN TRANSACTION

    Client->>Server: POST /gremlin<br/>Body: {"gremlin": "g.tx().begin()", "g": "gmodern"}
    
    Note right of Server: Generate transactionId
    
    Server-->>Client: 200 OK<br/>Body: {"transactionId": "abc-123-def-456", ...}

    Note over Client,Server: 2. EXECUTE OPERATIONS (repeat as needed)

    Client->>Server: POST /gremlin<br/>Header: X-Transaction-Id: abc-123-def-456<br/>Body: {"gremlin": "g.addV('person').property('name','josh')", "g": "gmodern", "transactionId": "abc-123-def-456"}
    
    Note right of Server: Lookup Transaction<br/>Execute on tx thread<br/>Reset timeout timer
    
    Server-->>Client: 200 OK + results

    Note over Client,Server: 3. COMMIT or ROLLBACK

    Client->>Server: POST /gremlin<br/>Header: X-Transaction-Id: abc-123-def-456<br/>Body: {"gremlin": "g.tx().commit()", "g": "gmodern", "transactionId": "abc-123-def-456"}
    
    Note right of Server: Commit graph transaction
    
    Server-->>Client: 200 OK
```

---