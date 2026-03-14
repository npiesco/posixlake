# Global Rules

## NEVER EVER DO ANY OF THESE:
- Never run a second terminal command while a build/test is in progress. One command at a time. Period.
- Never delete target. If there's a lock, delete only the lock file.
- Never echo/write into a terminal that has a running process.

## Terminal Commands (PowerShell)
- **NEVER** use `2>&1` redirection in PowerShell commands.
- **NEVER** use `*>` or any other redirection operator (`>`, `>>`, `2>`, `*>`, etc.).
- **NEVER** pipe terminal output through `Select-Object`, `Select-String`, or any other filter. Show full output.
- **NEVER** redirect output to files.
- **NEVER** guess paths, filenames, or command arguments. Look them up first.
- **NEVER** use `$LASTEXITCODE`, `echo "Exit code: $LASTEXITCODE"`, or any variant to check exit codes. PowerShell quirk: `echo` (Write-Output) always resets `$LASTEXITCODE` to **0**, so the value you read is **never** the real exit code. The terminal already reports the exit code — just read it from the terminal context. Do not try to capture, echo, or inspect it yourself.
- Run commands plainly: `cargo test`, `wasm-pack build`, `git push` — no piping, no redirection, no exit-code wrappers.

## Development Process
- **NO STUBS, NO MOCKS, NO TODO, NO LATER, NO FOR NOW, NO UNIT, NO DEFER.**
- Red/Green TDD everything. Write failing tests first, then implement.
- Use `cargo test --test <test_name>` for running a single isolated test file.
- Use `cargo nextest run` for the full regression suite (parallel, faster).
- `.cargo/config.toml` has `rustflags = ["-D", "warnings"]` — warnings are errors.
- `cargo` requires: `$env:PATH += ";$env:USERPROFILE\.cargo\bin"` in fresh terminals.
