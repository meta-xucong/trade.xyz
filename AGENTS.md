# AGENTS.md

Workspace-level guidance for the versioned trade.xyz repository.

## Layout

- `V1/` is the accepted V1 implementation. Treat it as stable unless the user
  explicitly asks for a V1 hotfix.
- `V2/` is the next-generation design and implementation workspace. New V2
  work must stay inside `V2/` unless a repository-level file such as this one
  must be updated.
- V1 and V2 must remain independent. Do not import V1 modules into V2 at
  runtime, do not share mutable config files, and do not move secrets between
  versions.

## Work Rules

- Read `V2/AGENTS.md` before implementing V2 code.
- Read `V1/AGENTS.md` before touching V1 code.
- Keep local secrets, vault files, logs, and build outputs untracked.
- Prefer preserving V1 as a known-good baseline while V2 evolves behind its own
  docs, config, tests, and binaries.

