#!/usr/bin/env bash
#
# pr-stack.sh — manage a stack of PR branches from a map file.
#
# In a stacked-PR workflow each PR branch points at a specific commit in a
# stack. A map file records "branch name -> commit message substring":
#
#     # comment lines and blank lines are ignored
#     my-feature-part-1    Add the widget model
#     my-feature-part-2    Wire widget into the API
#
# The first field is the PR branch name; the rest of the line (trimmed) is a
# substring to match against commit subjects.
#
# Subcommands:
#
#   remap [--apply] [--base <ref>] <map-file> <branch>
#       Locate each commit in <base>..<branch> by its message and move (or,
#       without --apply, just print) each PR branch onto its matching commit.
#       Use this after rebasing the stack to snap the PR branches back onto
#       their commits. Each substring must match exactly one commit or the
#       command aborts without touching any branch.
#
#   push [--remote <name>] [--no-force] [--dry-run] <map-file>
#       Push every branch named in the map to the remote, as-is (no
#       remapping). Verifies all branches exist locally first and aborts if
#       any are missing. Uses --force-with-lease unless --no-force is given.
#
#   capture [--base <ref>] [--output <file>] <branch>
#       The inverse of remap: reconstruct a map file from an existing stack.
#       Walks <base>..<branch> and, for each commit, records any local branch
#       that points at it (using the commit subject as the pattern). Handy
#       for snapshotting a stack you built by hand before a rebase.
#
set -euo pipefail

die() {
	printf 'error: %s\n' "$*" >&2
	exit 1
}

# Name of the currently checked-out branch; dies on detached HEAD.
current_branch() {
	local b
	b="$(git symbolic-ref --short -q HEAD)" ||
		die "detached HEAD — specify a branch explicitly"
	printf '%s' "$b"
}

usage() {
	cat >&2 <<'EOF'
Usage: pr-stack.sh <command> [options] <map-file> [args]

Commands:
  remap [--apply] [--base <ref>] <map-file> [<branch>]
        Move each PR branch onto the commit whose subject matches its map
        pattern in <base>..<branch> (default base: main, branch: current).
        Without --apply, only prints the plan (dry run).

  push [--remote <name>] [--no-force] [--dry-run] <map-file>
        Push every branch in the map to <remote> (default: origin) as-is.
        Verifies all branches exist locally first. Uses --force-with-lease
        unless --no-force is given.

  capture [--base <ref>] [--output <file>] [<branch>]
        Build a map from an existing stack: for each commit in
        <base>..<branch> (default branch: current), record any local branch
        whose tip is that commit, using the commit subject as its pattern.
        Writes to <file> or stdout.

Map file: one entry per line, "<branch-name>  <commit message substring>".
Blank lines and "#" comments are ignored.
EOF
	exit "${1:-0}"
}

# parse_map_file <map-file>
#
# Reads the map into the global parallel arrays MAP_BRANCHES and MAP_PATTERNS.
# Skips blank/comment lines. Aborts on a line that has a branch but no pattern.
MAP_BRANCHES=()
MAP_PATTERNS=()
parse_map_file() {
	local map_file="$1"
	[[ -f "$map_file" ]] || die "map file not found: $map_file"

	MAP_BRANCHES=()
	MAP_PATTERNS=()
	local line pr_branch pattern lineno=0
	while IFS= read -r line || [[ -n "$line" ]]; do
		lineno=$((lineno + 1))

		# Strip comments and surrounding whitespace; skip blanks.
		line="${line%%#*}"
		line="${line#"${line%%[![:space:]]*}"}" # ltrim
		line="${line%"${line##*[![:space:]]}"}" # rtrim
		[[ -n "$line" ]] || continue

		# First whitespace-delimited token is the branch; remainder is pattern.
		pr_branch="${line%%[[:space:]]*}"
		pattern="${line#"$pr_branch"}"
		pattern="${pattern#"${pattern%%[![:space:]]*}"}" # ltrim
		[[ -n "$pattern" ]] || die "$map_file:$lineno: no commit message pattern for branch $pr_branch"

		MAP_BRANCHES+=("$pr_branch")
		MAP_PATTERNS+=("$pattern")
	done <"$map_file"

	[[ ${#MAP_BRANCHES[@]} -gt 0 ]] || die "no usable entries in map file: $map_file"
}

cmd_remap() {
	local base="main" apply=0
	local positional=()
	while [[ $# -gt 0 ]]; do
		case "$1" in
		--apply) apply=1; shift ;;
		--base) base="${2:-}"; [[ -n "$base" ]] || die "--base requires a value"; shift 2 ;;
		-h | --help) usage 0 ;;
		--) shift; while [[ $# -gt 0 ]]; do positional+=("$1"); shift; done ;;
		-*) die "unknown option: $1" ;;
		*) positional+=("$1"); shift ;;
		esac
	done
	[[ ${#positional[@]} -ge 1 && ${#positional[@]} -le 2 ]] || usage 1
	local map_file="${positional[0]}"
	local branch="${positional[1]:-$(current_branch)}"

	git rev-parse --verify --quiet "$base^{commit}" >/dev/null || die "base ref not found: $base"
	git rev-parse --verify --quiet "$branch^{commit}" >/dev/null || die "branch not found: $branch"

	parse_map_file "$map_file"

	# Snapshot candidate commits once: "<hash><TAB><subject>" per line.
	local range="$base..$branch"
	local commits
	commits="$(git log --reverse --format='%H%x09%s' "$range")"
	[[ -n "$commits" ]] || die "no commits in range $range"

	local plan_hashes=() plan_subjects=()
	local errors=0 i pattern matches count hash subject
	for i in "${!MAP_BRANCHES[@]}"; do
		pattern="${MAP_PATTERNS[$i]}"

		# Literal substring match against subjects, case sensitive.
		matches="$(printf '%s\n' "$commits" | awk -F'\t' -v p="$pattern" 'index($2, p) { print }')" || true
		count=0
		[[ -n "$matches" ]] && count="$(printf '%s\n' "$matches" | grep -c '')"

		if [[ "$count" -eq 0 ]]; then
			printf 'error: no commit in %s matches "%s" (branch %s)\n' "$range" "$pattern" "${MAP_BRANCHES[$i]}" >&2
			errors=1; plan_hashes+=("") plan_subjects+=(""); continue
		fi
		if [[ "$count" -gt 1 ]]; then
			printf 'error: %d commits match "%s" (branch %s) — pattern is ambiguous:\n' "$count" "$pattern" "${MAP_BRANCHES[$i]}" >&2
			printf '%s\n' "$matches" | sed 's/\t/  /;s/^/         /' >&2
			errors=1; plan_hashes+=("") plan_subjects+=(""); continue
		fi

		hash="${matches%%$'\t'*}"
		subject="${matches#*$'\t'}"
		plan_hashes+=("$hash")
		plan_subjects+=("$subject")
	done

	[[ "$errors" -eq 0 ]] || die "aborting: some patterns did not match exactly one commit (no branches were moved)"

	printf 'Remap plan for stack %s (base %s):\n\n' "$branch" "$base"
	for i in "${!MAP_BRANCHES[@]}"; do
		printf '  %-48s -> %s  %s\n' "${MAP_BRANCHES[$i]}" "${plan_hashes[$i]:0:12}" "${plan_subjects[$i]}"
	done
	printf '\n'

	if [[ "$apply" -eq 0 ]]; then
		printf 'Dry run — re-run with --apply to move these branches.\n'
		return 0
	fi

	for i in "${!MAP_BRANCHES[@]}"; do
		git branch -f "${MAP_BRANCHES[$i]}" "${plan_hashes[$i]}"
		printf 'moved %s -> %s\n' "${MAP_BRANCHES[$i]}" "${plan_hashes[$i]:0:12}"
	done
	printf '\nDone. %d branch(es) remapped.\n' "${#MAP_BRANCHES[@]}"
}

cmd_push() {
	local remote="origin" dry_run=0 force=1
	local positional=()
	while [[ $# -gt 0 ]]; do
		case "$1" in
		--remote) remote="${2:-}"; [[ -n "$remote" ]] || die "--remote requires a value"; shift 2 ;;
		--no-force) force=0; shift ;;
		--dry-run) dry_run=1; shift ;;
		-h | --help) usage 0 ;;
		--) shift; while [[ $# -gt 0 ]]; do positional+=("$1"); shift; done ;;
		-*) die "unknown option: $1" ;;
		*) positional+=("$1"); shift ;;
		esac
	done
	[[ ${#positional[@]} -eq 1 ]] || usage 1
	local map_file="${positional[0]}"

	parse_map_file "$map_file"

	# Verify every branch exists locally before pushing anything.
	local missing=() i
	for i in "${!MAP_BRANCHES[@]}"; do
		git show-ref --verify --quiet "refs/heads/${MAP_BRANCHES[$i]}" ||
			missing+=("${MAP_BRANCHES[$i]}")
	done
	if [[ ${#missing[@]} -gt 0 ]]; then
		printf 'error: %d branch(es) from the map are missing locally:\n' "${#missing[@]}" >&2
		printf '         %s\n' "${missing[@]}" >&2
		die "aborting: nothing was pushed (run 'remap --apply' first?)"
	fi

	local push_opts=()
	[[ "$force" -eq 1 ]] && push_opts+=(--force-with-lease)

	printf 'Pushing %d branch(es) to %s%s:\n' "${#MAP_BRANCHES[@]}" "$remote" \
		"$([[ "$force" -eq 1 ]] && printf ' (--force-with-lease)')"
	for i in "${!MAP_BRANCHES[@]}"; do
		printf '  %s\n' "${MAP_BRANCHES[$i]}"
	done
	printf '\n'

	if [[ "$dry_run" -eq 1 ]]; then
		printf 'Dry run — re-run without --dry-run to push.\n'
		return 0
	fi

	git push "${push_opts[@]}" "$remote" "${MAP_BRANCHES[@]}"
	printf '\nDone. %d branch(es) pushed.\n' "${#MAP_BRANCHES[@]}"
}

cmd_capture() {
	local base="main" output=""
	local positional=()
	while [[ $# -gt 0 ]]; do
		case "$1" in
		--base) base="${2:-}"; [[ -n "$base" ]] || die "--base requires a value"; shift 2 ;;
		-o | --output) output="${2:-}"; [[ -n "$output" ]] || die "--output requires a value"; shift 2 ;;
		-h | --help) usage 0 ;;
		--) shift; while [[ $# -gt 0 ]]; do positional+=("$1"); shift; done ;;
		-*) die "unknown option: $1" ;;
		*) positional+=("$1"); shift ;;
		esac
	done
	[[ ${#positional[@]} -le 1 ]] || usage 1
	local branch="${positional[0]:-$(current_branch)}"

	git rev-parse --verify --quiet "$base^{commit}" >/dev/null || die "base ref not found: $base"
	git rev-parse --verify --quiet "$branch^{commit}" >/dev/null || die "branch not found: $branch"

	# The stack branch itself (resolved to its name even if passed as HEAD) and
	# the base are excluded from the captured keys.
	local self_name
	self_name="$(git rev-parse --abbrev-ref "$branch")"

	local range="$base..$branch"
	local commits
	commits="$(git log --reverse --format='%H%x09%s' "$range")"
	[[ -n "$commits" ]] || die "no commits in range $range"

	# Walk each commit oldest-first; record every local branch pointing at it.
	local cap_branches=() cap_subjects=()
	local width=0 hash subject b
	while IFS=$'\t' read -r hash subject; do
		while IFS= read -r b; do
			[[ -n "$b" ]] || continue
			[[ "$b" == "$self_name" || "$b" == "$base" ]] && continue
			cap_branches+=("$b")
			cap_subjects+=("$subject")
			[[ ${#b} -gt $width ]] && width=${#b}
		done < <(git branch --points-at "$hash" --format='%(refname:short)')
	done < <(printf '%s\n' "$commits")

	[[ ${#cap_branches[@]} -gt 0 ]] || die "no local branches point at commits in $range"

	# Render the map to a buffer, then emit to stdout or the output file.
	# Build with explicit newlines — $() would strip trailing ones.
	local nl=$'\n'
	local buf="# PR stack map for ${self_name} (base ${base})${nl}"
	buf+="#${nl}"
	buf+="# Format: <branch-name>  <commit-subject>${nl}"
	buf+="# Generated by scripts/pr-stack.sh capture.${nl}${nl}"
	local i
	for i in "${!cap_branches[@]}"; do
		buf+="$(printf '%-*s  %s' "$width" "${cap_branches[$i]}" "${cap_subjects[$i]}")${nl}"
	done

	if [[ -n "$output" ]]; then
		printf '%s' "$buf" >"$output"
		printf 'Captured %d branch(es) from %s -> %s\n' "${#cap_branches[@]}" "$range" "$output" >&2
	else
		printf '%s' "$buf"
	fi
}

[[ $# -gt 0 ]] || usage 1
command="$1"
shift
case "$command" in
remap) cmd_remap "$@" ;;
push) cmd_push "$@" ;;
capture) cmd_capture "$@" ;;
-h | --help) usage 0 ;;
*) die "unknown command: $command (expected 'remap', 'push', or 'capture')" ;;
esac
