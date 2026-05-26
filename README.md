# CDC Replication: PostgreSQL A -> PostgreSQL B

Real-time Change Data Capture (CDC) replication of a `products` table from a source PostgreSQL database to a target PostgreSQL database using **Debezium**, **Kafka**, and a **Python consumer**.

## Prerequisites

- Docker & Docker Compose
- TailScale tunnel between source and target machines
- The target machine's TailScale IP set as `TARGET_HOST` in `.env`

## Project Structure

```

├── compose.source.yml              # Source stack: Kafka, Debezium Connect, Postgres-A, replicator
├── compose.target.yaml             # Target stack: Postgres-B only
├── register-postgres-source.json   # Debezium connector config
├── sql/init.sql                    # Creates products table & publication
├── replicator/
│   ├── replicate.py                # Kafka consumer -> target DB upsert/delete
│   └── Containerfile               # Python 3.12 Docker image
└── .env                            # TARGET_HOST (TailScale IP)

```

## Setup & Running

### 1. Start the target database (on the target machine)

```bash
docker compose -f compose.target.yaml up -d
```

### 2. Start the source stack (on the source machine)

```bash
docker compose -f compose.source.yml up -d
```

### 3. Register the Debezium connector

```bash
curl -X POST -H "Content-Type: application/json" \
  -d @register-postgres-source.json \
  http://localhost:8083/connectors
```

## Testing the Replication

In **Postgres-A** (source), do a transaction:

```sql
INSERT INTO products (id, name, stock, price) VALUES (1, 'Laptop', 10, 15000000);
UPDATE products SET stock = 8 WHERE id = 1;
DELETE FROM products WHERE id = 1;
```

In **Postgres-B** (target), verify that the changes are replicated.

```sql
SELECT * FROM products;
```

## Cleanup

```bash
docker compose -f compose.source.yml down -v
docker compose -f compose.target.yaml down -v
```

## Tech Stack

| Component            | Technology                  |
|----------------------|-----------------------------|
| Source/Target DB     | PostgreSQL 17               |
| CDC Capture          | Debezium 3.5                |
| Message Broker       | Apache Kafka 3.5            |
| Replicator           | Python 3.12 (kafka-python, psycopg2) |
| Container Runtime    | Docker & Docker Compose     |
