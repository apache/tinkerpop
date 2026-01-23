# HTTP Transaction API Specification

## Overview

The Gremlin Server exposes a REST API for transaction management. Transactions are modeled as resources with unique IDs, and all operations within a transaction include the transaction ID in the URL path.

**NOTE** This is very preliminary and can easily be modeled by a single endpoint.

## API Endpoints

### Non-Transactional Traversal
```
POST /gremlin
```
Standard Gremlin Server endpoint for executing traversals without a transaction.

### Begin Transaction
```
POST /gremlin/tx

Response: 201 Created
Location: /gremlin/tx/{txId}
{
  "txId": "550e8400-e29b-41d4-a716-446655440000"
}
```
Creates a new transaction and returns a unique transaction ID.

### Execute Traversal Within Transaction
```
POST /gremlin/tx/{txId}

Response: 200 OK
(Standard Gremlin response)
```
Executes a traversal within the context of the specified transaction. Request body is the same as `POST /gremlin`, but the traversal is scoped to the transaction.

### Get Transaction Status
```
GET /gremlin/tx/{txId}

Response: 200 OK
{
  "txId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "ACTIVE"
}
```
Returns the current status of a transaction (ACTIVE, COMMITTED, or ROLLED_BACK).

### Commit Transaction
```
POST /gremlin/tx/{txId}/commit

Response: 200 OK
{
  "txId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "COMMITTED"
}
```
Commits all changes made within the transaction.

### Rollback Transaction
```
POST /gremlin/tx/{txId}/rollback

Response: 200 OK
{
  "txId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "ROLLED_BACK"
}
```
Rolls back all changes made within the transaction.

### Abort Transaction
```
DELETE /gremlin/tx/{txId}

Response: 200 OK
{
  "txId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "ROLLED_BACK"
}
```
Alternative to rollback - aborts the transaction and rolls back all changes.

## Error Responses

### Transaction Not Found
```
GET /gremlin/tx/invalid-id

Response: 404 Not Found
{
  "error": "Transaction not found",
  "txId": "invalid-id"
}
```

### Transaction Already in Terminal State
```
POST /gremlin/tx/{txId}/commit

Response: 409 Conflict
{
  "error": "Transaction already in terminal state",
  "txId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "COMMITTED"
}
```

### Transaction Execution Error
```
POST /gremlin/tx/{txId}

Response: 500 Internal Server Error
{
  "error": "Traversal execution failed",
  "message": "...",
  "txId": "550e8400-e29b-41d4-a716-446655440000"
}
```

## Transaction ID Format

- **Format**: UUID
- **Example**: `550e8400-e29b-41d4-a716-446655440000`
- **Generation**: Server-generated on `POST /gremlin/tx`
- **Usage**: Included in all subsequent requests as URL path parameter
- **Purpose**: Routes requests to correct transaction context on server

## Endpoint Summary Diagram

```mermaid
flowchart TD
    A[Client] -->|POST /gremlin| B[Non-Transactional Execution]
    A -->|POST /gremlin/tx| C[Begin Transaction]
    C -->|Returns txId| D[Transaction Active]
    D -->|POST /gremlin/tx/{txId}| E[Execute in Transaction]
    D -->|GET /gremlin/tx/{txId}| F[Get Status]
    D -->|POST /gremlin/tx/{txId}/commit| G[Commit]
    D -->|POST /gremlin/tx/{txId}/rollback| H[Rollback]
    D -->|DELETE /gremlin/tx/{txId}| H
    G --> I[Transaction Committed]
    H --> J[Transaction Rolled Back]
```
