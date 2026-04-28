#!/usr/bin/env bash
# Create a PR for the current commit and enable squash auto-merge.
#
# This keeps main protected from direct pushes while making the maintainer path:
#
#   scripts/pr-merge.sh docs/readme-update "Refresh README"

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/pr-merge.sh <branch-name> <pr-title> [pr-body-file]

What it does:
  1. Verifies the worktree is clean.
  2. Pushes the current HEAD to origin/<branch-name>.
  3. Creates a PR against main, or reuses the existing PR for that branch.
  4. Enables squash auto-merge and branch deletion.

Examples:
  scripts/pr-merge.sh docs/readme-update "Refresh README"
  scripts/pr-merge.sh fix/docker-build "Fix Docker build" /tmp/pr-body.md
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

branch="${1:-}"
title="${2:-}"
body_file="${3:-}"

if [[ -z "$branch" || -z "$title" ]]; then
  usage >&2
  exit 2
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "Worktree is dirty. Commit or stash changes before opening a PR." >&2
  git status --short >&2
  exit 1
fi

if ! gh auth status >/dev/null 2>&1; then
  echo "GitHub CLI is not authenticated. Run: gh auth login" >&2
  exit 1
fi

base_branch="$(gh repo view --json defaultBranchRef --jq '.defaultBranchRef.name')"
head_sha="$(git rev-parse --short HEAD)"

echo "Pushing $head_sha to origin/$branch..."
git push -u origin "HEAD:refs/heads/$branch"

pr_number="$(gh pr list --head "$branch" --json number --jq '.[0].number // empty')"

if [[ -z "$pr_number" ]]; then
  create_args=(--base "$base_branch" --head "$branch" --title "$title")
  if [[ -n "$body_file" ]]; then
    create_args+=(--body-file "$body_file")
  else
    create_args+=(--fill)
  fi
  pr_url="$(gh pr create "${create_args[@]}")"
  pr_number="${pr_url##*/}"
else
  pr_url="$(gh pr view "$pr_number" --json url --jq '.url')"
fi

echo "PR #$pr_number: $pr_url"
echo "Enabling squash auto-merge..."
gh pr merge "$pr_number" --squash --delete-branch --auto

echo "Auto-merge is queued. GitHub will merge when required checks pass."
