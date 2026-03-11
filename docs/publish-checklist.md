# Crate Publish Checklist

Follow these steps **in order** before running `cargo publish` for any crate in this workspace.

---

## 1. Scrub Private / Internal Content

- [ ] Search the entire repo for credential strings and internal hostnames:

  ```bash
  grep -r "TODO\|FIXME\|HACK\|INTERNAL\|PRIVATE" --include="*.rs" .
  ```

  Verify no matches appear in files that will be published.

---

## 2. Code Quality

- [ ] `cargo fmt --all` — no formatting diffs
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` — zero warnings
- [ ] `cargo test --workspace` — all non-ignored tests pass
- [ ] Run live integration tests (see `docs/development.md`):
  ```bash
  ADBC_POSTGRES_URI="…" ADBC_MYSQL_URI="…" cargo test --workspace -- --include-ignored
  ```

---

## 3. Documentation

- [ ] `cargo doc --workspace --no-deps` — builds without errors
- [ ] Every public `struct`, `trait`, `enum`, and `fn` has a `///` doc comment
- [ ] `lib.rs` module-level doc includes a brief description and at least one `# Example`
- [ ] Intra-doc links (`[`Type`]`) resolve correctly (check for broken links in `cargo doc` output)

---

## 4. Cargo Metadata (each crate's `Cargo.toml`)

- [ ] `[package].description` — one-line summary, no credentials
- [ ] `[package].license = "MIT OR Apache-2.0"`
- [ ] `[package].keywords` — up to 5 relevant keywords (e.g. `["adbc", "arrow", "database", "sqlite"]`)
- [ ] `[package].categories` — from the [crates.io category list](https://crates.io/category_slugs)
- [ ] `[package].repository` — GitHub URL
- [ ] `[package].homepage` — GitHub URL or docs.rs URL
- [ ] `[package].readme` — path to a per-crate `README.md` if one exists
- [ ] `[package].documentation` — `https://docs.rs/<crate-name>`
- [ ] `[package].version` — bumped appropriately per [semver](https://semver.org/)
- [ ] `[package].rust-version` — set to MSRV (currently `1.81`)

---

## 5. Dependency Hygiene

- [ ] No `path` dependencies remain without a matching `version` (Cargo requires version
      for published crates):
  ```toml
  # OK for published crates:
  adbc = { path = "../adbc", version = "0.1.0" }
  ```
- [ ] All `workspace.dependencies` have explicit version ranges (no `*`)
- [ ] Run `cargo tree --duplicates` and review any duplicate transitive deps
- [ ] Consider running `cargo audit` for known security advisories:
  ```bash
  cargo install cargo-audit
  cargo audit
  ```

---

## 6. Changelog / Release Notes

- [ ] `CHANGELOG.md` exists (or is created) with a section for this version
- [ ] All notable changes, new features, and breaking changes are documented

---

## 7. Dry Run

```bash
# Simulate publish without actually uploading
cargo publish --dry-run -p adbc
cargo publish --dry-run -p adbc-sqlite
cargo publish --dry-run -p adbc-postgres
cargo publish --dry-run -p adbc-mysql
cargo publish --dry-run -p adbc-flightsql
```

Check the output list of files that would be included in each package. Confirm no
sensitive files are present.

---

## 8. Publish Order

Publish in dependency order (dependents after dependencies):

```bash
cargo publish -p adbc           # core traits — no internal deps
# Wait for crates.io to index (usually ~30 seconds), then:
cargo publish -p adbc-sqlite
cargo publish -p adbc-postgres
cargo publish -p adbc-mysql
cargo publish -p adbc-flightsql
```

---

## 9. Post-Publish Verification

- [ ] `cargo add adbc` in a scratch project resolves from crates.io
- [ ] `https://docs.rs/<crate-name>` shows the rendered docs (may take a few minutes)
- [ ] GitHub release tag created: `git tag v0.1.0 && git push origin v0.1.0`
