# Architecture

How the operator, the Instance Manager and the Kubernetes objects of a
`RedisInstance` work together. The diagrams use the example instance
`cache` with `instances: 3` in namespace `app`.

## Resource tree (ownership)

Like the Argo CD resource tree: every arrow is an `ownerReference`
(`controller: true`), so deleting the `RedisInstance` garbage-collects
everything under it. Grey dashed boxes are not owned by the instance.

```mermaid
flowchart LR
    classDef cr fill:#e8d5ff,stroke:#7b3fbf,color:#000
    classDef pod fill:#d5f5e3,stroke:#1e8449,color:#000
    classDef svc fill:#d6eaf8,stroke:#2471a3,color:#000
    classDef secret fill:#fdebd0,stroke:#ca6f1e,color:#000
    classDef pvc fill:#fcf3cf,stroke:#b7950b,color:#000
    classDef rbac fill:#fadbd8,stroke:#b03a2e,color:#000
    classDef ext fill:#eaeded,stroke:#7f8c8d,color:#000,stroke-dasharray:4 3

    CA["Secret redis-operator-ca<br/>operator namespace"]:::ext
    RI["RedisInstance<br/>cache"]:::cr
    EXT["PVC from<br/>spec.storage.existingClaims"]:::ext

    RI --> SA["ServiceAccount cache-instance"]:::rbac
    RI --> ROLE["Role cache-instance<br/>get redisinstances/cache"]:::rbac
    RI --> RB["RoleBinding cache-instance"]:::rbac
    RI --> TLS["Secret cache-im-tls"]:::secret
    RI --> AUTH["Secret cache-internal-auth"]:::secret
    RI --> HL["Service cache-hl"]:::svc
    RI --> RW["Service cache-rw"]:::svc
    RI --> RO["Service cache-ro"]:::svc
    RI --> R["Service cache-r"]:::svc
    RI --> P0["Pod cache-0<br/>primary"]:::pod
    RI --> D0["PVC cache-0-data"]:::pvc
    RI --> P1["Pod cache-1<br/>replica"]:::pod
    RI --> D1["PVC cache-1-data"]:::pvc
    RI --> P2["Pod cache-2<br/>replica"]:::pod
    RI --> D2["PVC cache-2-data"]:::pvc

    CA -. signs .-> TLS
```

How the objects use each other:

| Object                      | Content                                         | Used by                                                  |
|-----------------------------|-------------------------------------------------|----------------------------------------------------------|
| Secret `redis-operator-ca`  | CA cert and key                                 | operator: signs `cache-im-tls` and its own client certs  |
| ServiceAccount `cache-instance` | token of the Instance Managers           | every Pod (`serviceAccountName`), redis container only   |
| Role / RoleBinding `cache-instance` | `get` on `redisinstances/cache` only  | the Instance Manager reads `status.currentPrimary`       |
| Secret `cache-im-tls`       | `tls.crt`, `tls.key`, `ca.crt`                  | every Pod, mounted at `/certificates`                    |
| Secret `cache-internal-auth`| `operator-password`, `replication-password`     | every Pod at `/internal-auth`; operator for `REPLICAOF`  |
| Service `cache-hl`          | headless                                        | stable DNS `cache-N.cache-hl.app.svc` for every Pod      |
| Service `cache-rw`          | selects `redis.operator.com/role=primary`       | clients that write                                       |
| Service `cache-ro`          | selects `redis.operator.com/role=replica`       | clients that only read                                   |
| Service `cache-r`           | selects every Pod                               | clients that read from any instance                      |
| PVC `cache-N-data`          | Redis data                                      | Pod `cache-N`, mounted at `/data`                        |

A PVC from `spec.storage.existingClaims` takes the place of the generated
`cache-N-data`; the operator never owns or deletes it. On scale-down the
operator deletes the extra Pods and their generated PVCs.

There is no ConfigMap: the Instance Manager builds `redis.conf` inside the
Pod from the mounted Secrets.

## Inside a Pod

```mermaid
flowchart TB
    classDef proc fill:#d5f5e3,stroke:#1e8449,color:#000
    classDef vol fill:#fcf3cf,stroke:#b7950b,color:#000
    classDef secret fill:#fdebd0,stroke:#ca6f1e,color:#000

    subgraph pod [Pod cache-1]
        direction TB
        INIT["initContainer bootstrap<br/>operator image<br/>/manager bootstrap"]:::proc

        subgraph redis [container redis - user image spec.image]
            IM["Instance Manager<br/>/controller/manager instance run<br/>PID 1"]:::proc
            RS["redis-server<br/>:6379"]:::proc
            PROBE[":8001 HTTP<br/>/healthz /readyz"]:::proc
            STATUS[":8000 HTTPS mTLS<br/>GET /status<br/>POST /replicaof"]:::proc
        end

        CTRL[("emptyDir /controller<br/>manager binary<br/>redis.conf 0600")]:::vol
        DATA[("/data<br/>PVC<br/>.operator/epoch")]:::vol
        CERTS[("/certificates<br/>Secret cache-im-tls")]:::secret
        IAUTH[("/internal-auth<br/>Secret cache-internal-auth")]:::secret
        CG[("/sys/fs/cgroup/memory.max<br/>limit from spec.resources")]:::vol
        TOKEN[("/var/run/secrets/kubernetes.io/serviceaccount<br/>projected token of cache-instance")]:::secret
    end
    API["Kubernetes API<br/>RedisInstance cache"]

    INIT -- copies itself --> CTRL
    CTRL -- runs binary --> IM
    IAUTH -- passwords --> IM
    CG -- memory limit --> IM
    TOKEN -- token --> IM
    IM -- "get status.currentPrimary<br/>before redis-server starts" --> API
    IM -- writes redis.conf --> CTRL
    IM -- starts, SIGTERM on shutdown --> RS
    IM --- PROBE
    IM --- STATUS
    CERTS -- reloaded on every handshake --> STATUS
    PROBE -- PING as _operator --> RS
    STATUS -- INFO replication, REPLICAOF as _operator --> RS
    STATUS -- highest accepted epoch --> DATA
    RS --- DATA
```

The init container copies the operator binary into a shared `emptyDir`, so
the redis container can run the Instance Manager with any Redis image.

## Safe start

Before it starts `redis-server`, the Instance Manager reads
`status.currentPrimary` of its own `RedisInstance` (waiting until the operator
has set it):

- **This Pod is the primary:** `redis.conf` has no `replicaof`.
- **Another Pod is:** `redis.conf` gets `replicaof <primary>.cache-hl.app.svc 6379`,
  so a restarted replica never runs as a primary, not even for a moment, and
  takes no writes on a stale dataset.

The Pod keeps `automountServiceAccountToken: false`. The operator mounts a
projected token of `cache-instance` into the redis container only, at the path
client-go expects, so the init container gets no token. The Role allows `get`
on this one `RedisInstance` and nothing else.

`redis.conf` turns off the passwordless `default` user (unless
`spec.acl.defaultUser: enabled`) and defines two internal users:

| User           | Rights                         | Used by                                      |
|----------------|--------------------------------|----------------------------------------------|
| `_operator`    | `+@all`                        | Instance Manager                              |
| `_replication` | `+psync +replconf +ping`       | replicas syncing from the primary            |

## Memory

Redis keeps the whole dataset in RAM, so `maxmemory` must fit the container
memory limit. Without it Redis grows until the kernel OOMKills it, instead of
rejecting writes or evicting keys.

```mermaid
flowchart LR
    classDef cr fill:#e8d5ff,stroke:#7b3fbf,color:#000
    classDef k8s fill:#d6eaf8,stroke:#2471a3,color:#000
    classDef pod fill:#d5f5e3,stroke:#1e8449,color:#000

    SPEC["spec.resources.limits.memory: 128Mi<br/>spec.redis.maxmemoryRatio: 75<br/>spec.redis.maxmemoryPolicy: noeviction"]:::cr
    OP["Operator"]:::k8s
    POD["redis container<br/>resources + flags<br/>--maxmemory-ratio=75<br/>--maxmemory-policy=noeviction"]:::k8s
    CG["cgroup memory.max<br/>134217728, or max"]:::k8s
    IM["Instance Manager<br/>128Mi x 75 / 100"]:::pod
    CONF["redis.conf<br/>maxmemory 100663296<br/>maxmemory-policy noeviction"]:::pod
    STATUS["status.conditions<br/>MemoryLimited"]:::cr

    SPEC --> OP --> POD
    POD -- kubelet --> CG --> IM
    POD -- flags --> IM
    IM --> CONF
    OP -- checks the admitted Pods --> STATUS
```

- **`maxmemory = memory limit × maxmemoryRatio / 100`.** The ratio defaults
  to 75 and may be 10–95. The rest is left for fork copy-on-write (BGSAVE,
  AOF rewrite), replication and client buffers, and fragmentation.
- **The limit comes from cgroup v2**, not the Downward API: without a limit
  `resourceFieldRef: limits.memory` gives the node memory, while
  `memory.max` gives `max`. The cgroup also shows a default limit set by a
  LimitRange. Nodes with cgroup v1 are not supported.
- **`maxmemoryPolicy`** defaults to `noeviction`: Redis rejects writes
  instead of silently deleting keys. Pick `allkeys-lru` or `allkeys-lfu` for a
  cache.
- **No memory limit:** `maxmemory 0` (no limit). The instance still starts,
  for development, but the condition `MemoryLimited` turns `False` and the
  operator emits one `Warning` Event `MemoryLimitMissing`.
- **Changes** reach only new Pods for now: the Instance Manager writes the
  settings to `redis.conf` at start. Applying them live with `CONFIG SET`
  comes with the rolling update (step 3).

## Runtime cooperation

```mermaid
flowchart LR
    classDef op fill:#e8d5ff,stroke:#7b3fbf,color:#000
    classDef k8s fill:#d6eaf8,stroke:#2471a3,color:#000
    classDef pod fill:#d5f5e3,stroke:#1e8449,color:#000
    classDef user fill:#eaeded,stroke:#7f8c8d,color:#000

    subgraph opns [namespace redis-operator-system]
        OP["Operator<br/>controller-manager"]:::op
    end

    API["Kubernetes API"]:::k8s
    KUBELET["kubelet"]:::k8s
    APP["Client application"]:::user

    subgraph appns [namespace app]
        subgraph p0 [Pod cache-0 primary]
            IM0["Instance Manager"]:::pod
            RS0["redis-server"]:::pod
        end
        subgraph p1 [Pod cache-1 replica]
            IM1["Instance Manager"]:::pod
            RS1["redis-server"]:::pod
        end
    end

    OP <-- "watch RedisInstance + owned objects<br/>create Secrets, Services, PVCs, Pods<br/>patch role label, update status" --> API
    API -- schedules --> KUBELET
    KUBELET -- "probes :8001" --> IM0 & IM1
    OP -- "POST /replicaof :8000<br/>mTLS, client CN=redis-operator" --> IM1
    IM1 -- "REPLICAOF cache-0... <br/>user _operator" --> RS1
    RS1 -- "PSYNC :6379<br/>user _replication" --> RS0
    APP -- "cache-rw :6379" --> RS0
    APP -- "cache-ro :6379" --> RS1
```

## Epoch

`status.primaryEpoch` grows with every change of the primary (failover comes
in step 4; until then it stays `1`). Every command of the operator to an
Instance Manager carries it:

- The Instance Manager stores the highest epoch it has accepted on the PVC
  (`/data/.operator/epoch`, written atomically with fsync and rename), so it
  survives restarts of the Pod.
- A command with a lower epoch gets `409 Conflict` and is not run. This stops
  a late request of a former operator leader from undoing a failover.
- Commands run one at a time, so one of an older epoch never runs after a
  newer one.

The operator no longer connects to Redis itself: `REPLICAOF` goes through
`POST /replicaof`, which is idempotent and does nothing when the replica
already follows the requested primary.

## Reconcile loop

One pass of `RedisInstanceReconciler.Reconcile`. The operator re-runs it on
every change of the `RedisInstance` or an owned object, and requeues after
5 s while replication is not set up.

```mermaid
sequenceDiagram
    autonumber
    participant OP as Operator
    participant API as Kubernetes API
    participant IM as Instance Manager (replica)
    participant RS as redis-server (replica)

    Note over OP: On start: EnsureCA loads or creates<br/>Secret redis-operator-ca
    OP->>API: Get RedisInstance
    OP->>API: CreateOrUpdate Secret cache-im-tls<br/>(new cert when missing or past 2/3 of lifetime)
    OP->>API: CreateOrUpdate Secret cache-internal-auth<br/>(generate only missing passwords)
    OP->>API: CreateOrUpdate ServiceAccount, Role, RoleBinding cache-instance
    OP->>API: Ensure Services hl, rw, ro, r
    loop each ordinal
        OP->>API: Ensure PVC (unless existingClaim)
        OP->>API: Create Pod, or patch its role label
    end
    OP->>API: Delete Pods and generated PVCs above spec.instances
    loop each Ready replica
        OP->>IM: POST /replicaof {epoch, primaryHost} (mTLS)
        alt epoch lower than the one IM accepted
            IM-->>OP: 409 Conflict
        else
            IM->>IM: store epoch in /data/.operator/epoch
            IM->>RS: INFO replication
            opt not replicating from cache-0
                IM->>RS: REPLICAOF cache-0.cache-hl.app.svc 6379
            end
            IM-->>OP: {changed}
        end
    end
    OP->>API: Update status if changed (readyInstances, phase, currentPrimary,<br/>primaryEpoch, MemoryLimited condition)
    opt a Pod has no memory limit (first time)
        OP->>API: Warning Event MemoryLimitMissing
    end
```
