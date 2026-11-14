# Security — Credit Risk Underwriting Platform

This document describes the demo security controls and what is still required for a real production deployment.

## Authentication

- Passwords hashed with BCrypt (never stored in plaintext).
- Short-lived JWT access tokens (HMAC-SHA256, default 15 minutes) in the `Authorization: Bearer` header.
- Opaque refresh tokens stored as SHA-256 hashes in memory.
- Refresh tokens delivered as `HttpOnly`, `SameSite=Strict` cookies scoped to `/api/auth`.
- Refresh requires header `X-Refresh-Request: 1` in addition to the cookie.
- Refresh tokens rotate on every use; presenting a previously rotated token revokes that user’s refresh sessions.
- Logout revokes the current refresh token and clears the cookie.
- Demo users are initialized from environment variables (`DEMO_*`). They are not hardcoded in source.

## Authorization (RBAC)

| Role | Capabilities |
| --- | --- |
| `ADMIN` | All dashboard APIs, `/api/health`, `/api/admin/users` |
| `RISK_ANALYST` | Overview, segments, scoring, application lookup |
| `UNDERWRITER` | Overview, segments, scoring, application lookup |

Authorization is enforced in Spring Security, not only in the UI.

## API protection

- CORS allowlist from `CORS_ALLOWED_ORIGINS` (no wildcard).
- Login rate limiting and temporary lockout.
- Scoring rate limiting per user/IP.
- Request body size limits on backend and ML service.
- Generic authentication error messages.
- No stack traces in API error responses.
- Security headers: `X-Content-Type-Options`, `X-Frame-Options`, `Referrer-Policy`, `Permissions-Policy`.
- Actuator exposes only `/actuator/health` with details disabled.
- Swagger UI enabled only on the `local` profile and requires authentication.

## Applicant data protection

- Applicant payloads are not logged by the backend or ML adapter.
- Application lookup returns a narrow field set (id, PD, tier, action, limited demographics).
- Model internals beyond top SHAP drivers are not returned.
- Synthetic/demo applicant profiles are used for demonstrations.

## Python ML service protection

- Listens on the Docker `internal` network only (no published host port in Compose).
- Requires shared `X-Internal-Token` from `ML_SERVICE_TOKEN`.
- Validates non-empty applicant objects and rejects oversized bodies.
- Docs/OpenAPI disabled on the adapter.

## Secrets management

- Secrets live in `.env` (gitignored). `.env.example` contains placeholders only.
- `JWT_SECRET` and `ML_SERVICE_TOKEN` must be set or the services refuse to start.
- Vite never receives backend secrets.
- Production profile refuses demo passwords containing `change-me` / `placeholder`.

## CSRF notes

Access tokens are Bearer tokens (not cookies), so classic cookie CSRF does not apply to most API calls. Refresh uses a cookie plus a custom header. This is appropriate for the SPA demo; a production deployment should reassess cookie policies under HTTPS and consider additional CSRF defenses if cookie-based auth expands.

## Production requirements (not claimed by this demo)

JWT and RBAC alone do not make the system production-secure. Before any real deployment you still need:

- TLS termination and HTTPS-only cookies (`Secure` flag)
- Durable session/refresh store (not in-memory)
- Secret rotation and a secrets manager
- External identity provider / SSO for staff accounts
- Centralized audit logging
- WAF / network policies and private ML networking in a real VPC
- Penetration testing and dependency vulnerability scanning
- Data retention and privacy reviews for applicant information

This project is a university demonstration of an integrated underwriting stack with serious baseline controls, not a certified production banking platform.
