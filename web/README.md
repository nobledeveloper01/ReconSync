# Dashboard

Vite + TypeScript, no runtime dependencies. Built into `dist/`, copied to
`embed/dist/`, and served by the ReconSync binary itself at `/`.

```bash
make web        # install, build, and stage for embedding
cd web && npm run dev    # dev server, proxying /v1 to a local ReconSync on :8080
```

`embed/dist/` is committed so `go build` never needs node installed. A
contributor who does not touch the dashboard builds and tests exactly as before.

Three decisions worth knowing:

- **Served from the same origin as the API.** A dashboard hosted elsewhere would
  need CORS opened on an endpoint that advises money movement, and would put the
  customer's key through a cross-origin request. Same origin needs neither.
- **The key lives in `sessionStorage`,** so it is gone when the tab closes. A key
  that outlives the session on a shared machine is a credential left lying
  around, and this one reads every transaction a tenant has.
- **Asset filenames are content-hashed,** which is what makes the year-long
  immutable cache the server sends correct. Pinning them to `app.js` while
  serving `immutable` would leave every browser on the old app after a deploy —
  which is exactly what happened during development, and cost an hour.
