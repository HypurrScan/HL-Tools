# eMudhra Python trust-store update — 2026-10-28

Type: runbook.

## Purpose and source of truth

`requirements-security.txt` requires `certifi>=2026.10.19` for TLS clients.
The [upstream release](https://github.com/certifi/python-certifi/releases/tag/2026.10.19)
removes the affected emSign root. Other application dependencies are unchanged.

## Existing environments

The operator MUST use the Python executable of the existing multisend environment.
This repository has no complete dependency lockfile. `requirements-security.txt`
adds only the affected trust-store minimum. The operator MUST NOT run
`python/multisend.py` as a verification step because it can send real transactions.
The operator SHOULD upgrade only the affected package, rather than reinstalling
all application dependencies in a live environment.

```sh
/path/to/service/venv/bin/python -m pip install --upgrade 'certifi>=2026.10.19'
/path/to/service/venv/bin/python -m pip check
/path/to/service/venv/bin/python -c 'import certifi; print(certifi.__version__, certifi.where())'
```

A version number alone does not verify an overridden or modified bundle.
The operator MUST audit the actual PEM bundle, custom TLS paths and system CA
store using the fleet eMudhra audit before declaring trust acceptance.
The operator MUST audit pip's vendored CA bundle separately.
The operator MUST NOT install packages into Debian's system Python with pip.

The operator MUST restart only active consumers, one service at a time, using
their existing systemd/tmux procedure and preserving their environment, data
and checkpoint files. Already accepted environments need no repeat restart.
Retired applications MUST NOT be relaunched for this repair.
The operator SHOULD keep the previous package inventory and artifact for
rollback; restoring an affected bundle reopens the trust-store issue.

## Limits

This PR changes dependency policy only. It does not deploy, restart, send
notifications or establish acceptance for a running environment.
