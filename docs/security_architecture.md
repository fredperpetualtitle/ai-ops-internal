# Security Architecture (One Page)

## Scope
- FastAPI public API on Railway
- ChromaDB private service on Railway with persistent volume
- Custom GPT calls FastAPI only; Chroma never public

## Target Architecture
- Internet -> TLS -> FastAPI (public)
- FastAPI -> Railway private network -> ChromaDB (private)
- ChromaDB -> Railway volume at /data/chroma

## Auth and Access
- Phase 1: API key required on all non-public endpoints
- Phase 2: JWT for identity plus HMAC request signatures for sensitive routes
- Rotate secrets quarterly; invalidate JWTs on rotation

## Networking
- ChromaDB has no public URL
- FastAPI connects to ChromaDB via internal Railway hostname
- Public TLS handled by Railway edge; internal traffic stays private

## Data Protection
- Store embeddings plus metadata; store full text only when required for retrieval
- No raw email content in logs
- Environment variables only, no secrets in code or files

## Logging and Observability
- Log request metadata and hashes only
- Redact sensitive fields (email bodies, attachments)
- Record request_id, intent, route, and result counts

## Backups and Retention
- Daily backups, weekly and monthly retention
- Monthly restore test
- Retention targets: emails/transcripts 6 months, embeddings 12 months

## Rate Limiting and Abuse Controls
- Per-key rate limiting on public endpoints
- Max results per query (default 5 to 10)
- Mandatory metadata filters when possible (entity and time window)

## Go-Live Checklist
- API key enforcement enabled
- CORS restricted to approved origins
- Chroma private service only (no public URL)
- Secrets stored in Railway variables
- Logging redaction verified
- Backup and restore test completed

## Open Decisions
- JWT and HMAC rollout timing
- Final retention policy per data source
- Read.ai ingestion and transcript handling rules
