# faker

Faker models a cluster of nodes, where services are deployed, exposing an API that
is used by clients to interact with.

## Overview

```mermaid
graph LR
F[Frontend]-->A[API]
A-->B[Backend]
B-->C[Cache]
B-->D[DB]
```

```mermaid
sequenceDiagram
Frontend->>API: HTTP GET /
API->>Backend: gRPC Get()
Backend->>Cache: INCR counter
Cache-->>Backend: counter
Backend->>DB: SELECT * FROM table
DB-->>Backend: posts (psql)
Backend-->>API: posts (pb)
API-->>Frontend: posts (json)
```

## Services

### Frontend

Represents a web browser.

#### Static
- IP address
- User agent
- Target web domain

#### Dynamic
- External port

### API

Represents a web server that exposes an HTTP API.
Accepts an HTTP request, forwards it to backend via gRPC and
returns the response in json format.

#### Static
- IP address
- Server

#### Dynamic
- External port
