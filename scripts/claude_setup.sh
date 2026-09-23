#!/usr/bin/env bash
# Idempotent per-machine Claude Code setup for this repo.
#
# .claude/settings.json is the source of truth: extraKnownMarketplaces are
# registered (Claude Code also auto-registers them once the folder is trusted,
# so this is belt-and-braces for scripted/headless setup) and every plugin in
# enabledPlugins is installed at project scope. Marketplace registration and
# the enabledPlugins list replicate via settings.json, but plugin *installs*
# are per-machine state — this script replays them on a fresh box.
#
# Usage: scripts/claude_setup.sh   (from anywhere inside the repo)
set -euo pipefail

for tool in claude jq; do
    command -v "$tool" >/dev/null || { echo "error: '$tool' not on PATH" >&2; exit 1; }
done

repo_root=$(git rev-parse --show-toplevel)
settings="$repo_root/.claude/settings.json"
[[ -f $settings ]] || { echo "error: $settings not found" >&2; exit 1; }

# --- marketplaces -----------------------------------------------------------
# `claude plugin marketplace list` has no --json; probe its human output for
# the exact name. Registration keys on the name, adding uses the repo slug.
registered=$(claude plugin marketplace list 2>/dev/null || true)

while IFS=$'\t' read -r name repo; do
    if grep -q "❯ ${name}\$" <<<"$registered"; then
        echo "marketplace ${name}: already registered"
    else
        echo "marketplace ${name}: adding from ${repo}"
        claude plugin marketplace add "$repo"
    fi
done < <(jq -r '.extraKnownMarketplaces // {} | to_entries[]
                | [.key, .value.source.repo] | @tsv' "$settings")

# --- plugins ----------------------------------------------------------------
# `claude plugin list --json` may print informational lines before the JSON;
# keep everything from the first line that starts the JSON document onward.
installed=$(claude plugin list --json 2>/dev/null | awk '/^[\[{]/{found=1} found' \
    | jq -r '.[].id' 2>/dev/null || true)

while read -r plugin; do
    if grep -qx "$plugin" <<<"$installed"; then
        echo "plugin ${plugin}: already installed"
    else
        echo "plugin ${plugin}: installing (project scope)"
        claude plugin install "$plugin" --scope project
    fi
done < <(jq -r '.enabledPlugins // {} | to_entries[] | select(.value) | .key' "$settings")

echo "done."
