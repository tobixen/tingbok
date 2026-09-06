# Tingbok deployment

Tingbok runs on `broxbox06.rl_tobias.c.bitbit.net` (NixOS, OpenStack) behind nginx at
`https://tingbok.plann.no`.

## How the service is managed

The NixOS module (`myopenstacknixos/configurations/profiles/tingbok.nix`) creates three
systemd units:

| Unit | Purpose |
|---|---|
| `tingbok-setup.service` | First-boot: `git clone` + `python3 -m venv` + `pip install` |
| `tingbok-update.service` | Periodic: self-heal → `git merge origin/main` → `git push` → `pip install` → `systemctl restart tingbok` |
| `tingbok-update.timer` | Fires `tingbok-update` 5 min after boot, then every 15 min |
| `tingbok.service` | Runs `uvicorn tingbok.app:app` on `127.0.0.1:5100` |

**Deploying a code or data change** is therefore just:

```bash
git push origin main   # push to GitHub
```

The server picks it up within 15 minutes automatically.  To deploy immediately:

```bash
ssh broxbox06.rl-tobias.c.bitbit.net sudo systemctl start tingbok-update
```

## Repo layout on the server

```
/opt/tingbok/repo/   — git clone of github.com/tobixen/tingbok (branch: main)
/opt/tingbok/venv/   — Python virtualenv, editable install of the repo
/var/cache/tingbok/  — SKOS cache, EAN lookups, GPT taxonomy cache
/etc/tingbok/deploy_key  — SSH private key for git push-back (secret, not in Nix store)
```

## Git push-back

The server holds an SSH deploy key for `tobixen/tingbok` on GitHub.  After each `git pull`,
`tingbok-update` also runs `git push`, so EAN entries or data commits made on the server
are synced back to GitHub automatically.

The key is at `/etc/tingbok/deploy_key` (owned by the `tingbok` system user).
The push URL for the origin remote is set to SSH (`git@github.com:tobixen/tingbok.git`)
by the `tingbok-setup` service on first boot.

## Why the server merges and never rebases

The service auto-commits into the same repo it pulls, so a pull reconciles two real
histories. `git pull --rebase` was fatal here: a conflict leaves the repo on a
**detached HEAD** with the rebase half-finished, the service happily keeps
auto-committing onto that detached HEAD, and every later update fails with *"You are
not currently on a branch"*. That silently stranded 62 commits for seven weeks in
June–August 2026.

Three things now prevent a repeat:

1. **Merge, not rebase.** A merge cannot detach HEAD, and it never rewrites commits
   that were already made — and possibly pushed — on the server.
2. **A semantic merge driver for `ean-db.json`.** Both sides append observations to
   that one file, so a *textual* merge conflicts on nearly every pull. `.gitattributes`
   marks it `merge=ean-db` and `tingbok-setup` registers the driver:

   ```sh
   git config merge.ean-db.name "semantic merge of ean-db.json"
   git config merge.ean-db.driver \
     "/opt/tingbok/venv/bin/python /opt/tingbok/repo/scripts/merge_ean_db.py --merge-driver %O %A %B"
   ```

   Note the **venv** interpreter: the script imports `tingbok.services.ean` to share
   the service's own merge rules, which needs the venv's dependencies. A bare
   `python3` dies on `ImportError` and git falls back to a text conflict.

   Registering the driver is worth doing in your own clone too — the same
   `git config` lines, with paths adjusted.
3. **Self-healing.** Before pulling, `tingbok-update` aborts any half-finished
   rebase/merge/cherry-pick and reattaches a detached HEAD. If the detached commits
   descend from the branch it fast-forwards the branch onto them; if they have
   diverged it parks them on a `rescue/detached-<sha>` branch rather than orphaning
   them. Either way nothing is discarded.

If the driver ever cannot resolve a merge, `tingbok-update` aborts it, exits non-zero
and leaves the repo on a clean branch, so the service keeps recording observations
locally while you sort it out. `scripts/merge_ean_db.py --ours <ref> --theirs <ref>`
does the same reconciliation by hand.

To rotate the deploy key:

```bash
# Generate new key
ssh-keygen -t ed25519 -C "tingbok@broxbox06" -N "" -f /tmp/new_deploy_key

# Replace on GitHub
gh api repos/tobixen/tingbok/keys --method POST \
  -f title="broxbox06 deploy key (new)" \
  -f key="$(cat /tmp/new_deploy_key.pub)" \
  -F read_only=false
# Then delete the old key via: gh api repos/tobixen/tingbok/keys

# Deploy to server
cat /tmp/new_deploy_key | ssh broxbox06.rl-tobias.c.bitbit.net \
  'sudo tee /etc/tingbok/deploy_key > /dev/null &&
   sudo chmod 400 /etc/tingbok/deploy_key &&
   sudo chown tingbok:tingbok /etc/tingbok/deploy_key'
```

## NixOS config location

The NixOS configuration lives in a separate repo:
`~/myopenstacknixos` → `configurations/broxbox06.rl_tobias.c.bitbit.net.nix`

After editing, push to srv1 and rebuild on the VM:

```bash
cd ~/myopenstacknixos
git push srv1 main
ssh broxbox06.rl-tobias.c.bitbit.net sudo nixos-rebuild switch
```

## Checking service status

The quickest check needs no ssh, because `/health` reports the updater's own state:

```bash
curl -s https://tingbok.plann.no/health | jq '.status, .update'
```

`status` is `degraded` — the service itself still answers — whenever the venv is behind
the checkout, the last update run failed, or no run has happened for
`stale_after_seconds` (a run killed before it could record anything, a stopped timer, a
unit that will not start):

```json
{
  "repo_rev": "5755391",
  "venv_rev": "2686388",
  "install_failures": 4,
  "stage": "pip",
  "last_error": null,
  "last_attempt": "2026-09-05T14:30:04+02:00",
  "last_success": "2026-09-05T12:00:03+02:00"
}
```

`last_error` is `null` for anyone but a localhost client: pip and git quote filesystem
paths in their errors, and that is what the `paths` block is withheld for.  Read it on
the VM, where the whole block is served:

```bash
ssh broxbox06.rl-tobias.c.bitbit.net \
  "curl -s http://127.0.0.1:5100/health | jq -r '.update.last_error'"
```

This exists because a failed update is otherwise silent: `tingbok.service` keeps serving
from the code it imported at startup, so the breakage only surfaces at the next restart.
`tingbok-update` writes the state via `scripts/update_status.py` to the file named by
`TINGBOK_UPDATE_STATUS_FILE` (`/opt/tingbok/update-status.json` on the VM); off a managed
host the variable is unset and `/health` reports no `update` block at all.

For the detail behind a `degraded`:

```bash
ssh broxbox06.rl-tobias.c.bitbit.net '
  systemctl status tingbok tingbok-update.timer
  journalctl -u tingbok -n 30
  journalctl -u tingbok-update -n 20
'
```

## Anything fronting tingbok must forward X-Forwarded-For

`/health` withholds its `paths` block (module, executable, data and cache dirs) from
anyone but a localhost client, and decides that from `request.client.host`.  Behind a
reverse proxy that is the *proxy's* address unless the proxy forwards the real one:
uvicorn's proxy-header middleware is on by default and trusts `127.0.0.1`, but it only
rewrites the client when an `X-Forwarded-For` header is actually present.

nginx sends one only when `recommendedProxySettings` is on, which sets
`X-Forwarded-For $proxy_add_x_forwarded_for`.  On broxbox06 it always has been —
`services.nginx.recommendedProxySettings = true` is set globally by the matrix-server and
chat-hub profiles the host also imports, and the per-location option inherits that — so
the block has not in fact been exposed.  `roles/tingbok-server.nix` now sets it on the
tingbok location explicitly all the same, because inheriting it from an unrelated profile
means a host that imports the tingbok role *alone* would serve the block to the internet,
and nothing about the role says so.

Because `$proxy_add_x_forwarded_for` *appends* the real peer and uvicorn reads the list
from the right, a client sending its own `X-Forwarded-For: 127.0.0.1` cannot forge its way
in.  A direct request to `127.0.0.1:5100` on the VM sends no such header at all, so it is
unaffected and still gets the full block.  Any other proxy put in front of tingbok needs
the equivalent setting.

There is a second way in, and it is why the gate refuses `::1`.  uvicorn's middleware
trusts the IPv4 literal `127.0.0.1` and nothing else by default, so a proxy that reaches
uvicorn over IPv6 loopback — `proxy_pass` to a `localhost` that resolves to `::1`, or
uvicorn bound to `::` — is not a trusted peer at all.  Its `X-Forwarded-For` is then
ignored however correctly nginx sets it, and every request from the internet arrives with
`request.client.host == "::1"`.  Trusting that address would hand the block to the world
again, so tingbok does not: only IPv4 loopback opens the gate, including the
`::ffff:127.0.0.1` form a dual-stack listener reports.  The cost is that a direct request
over IPv6 loopback gets the public response — use `127.0.0.1`, or add `::1` to uvicorn's
`--forwarded-allow-ips` *and* to the gate, together and deliberately.

## Secrets required (not in git)

| Path on VM | Purpose | Deploy script |
|---|---|---|
| `/etc/acme/rfc2136.env` | Let's Encrypt DNS-01 TSIG credentials | `myopenstacknixos/setup-broxbox06-secrets.sh` |
| `/etc/tingbok/deploy_key` | GitHub SSH deploy key | see above |
