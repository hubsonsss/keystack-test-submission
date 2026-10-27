# Keystack

API key management for an AI inference service.

## Run

```
docker compose up --build
```

- Frontend: http://localhost:3000
- Backend: http://localhost:8080 (docs at /docs)

## Example

```
curl -X POST localhost:8080/create_customer -H 'Content-Type: application/json' -d '{"name": "Acme"}'
curl -X POST localhost:8080/customers/1/keys -H 'Content-Type: application/json' -d '{}'
curl -X POST localhost:8080/v1/inference -H 'Authorization: Bearer <secret>'
```

## Notes

- Python + FastAPI, Postgres, Redpanda, plain JS frontend.
- One customer has many keys.
- Key events go to the `key-events` topic and end up in the `audit_log` table.
