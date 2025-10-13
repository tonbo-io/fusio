# Fusio Examples

This crate collects runnable snippets for the broader Fusio stack (filesystem,
object-store adapters, alternative runtimes, …).

To explore the remaining samples in this package, enable the runtime you are
interested in (e.g., `cargo run -p examples --features tokio --bin ...`).

**Note:** Manifest-specific workflows now live directly under the
`fusio-manifest` crate so they can track the API one to one — run them with
`cargo run -p fusio-manifest --example <name>`.
