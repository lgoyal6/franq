# Franq public lab

Native Go scheduling logic, synthetic jobs, browser memory only. No durable enqueue, WAL, lease, or crash-recovery behavior is represented here.

Build from the repository root with `bash docs/public-demo/build.sh`, then serve `docs/public-demo` with a static HTTP server. Generated Go WASM assets are ignored; use the matching Go toolchain wasm_exec.js. Publish only this directory as the Vercel project root, using its vercel.json. It contains no application API or service credentials.

The existing native broker implements persistence and visibility leases separately. Browser ordering is not a durability benchmark.

Input limits are enforced by the local demo engine. Reset or reload discards state. CSP limits scripts, workers, and connections to this origin. No dependencies are loaded from a CDN.

After building, run `node docs/public-demo/check.cjs` from the repository root. It exercises the compiled native engine with positive and rejection controls without a server or network call.
