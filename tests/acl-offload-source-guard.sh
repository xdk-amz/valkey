#!/usr/bin/env bash
# acl-offload structural guard.
#
# Every raw store to a user's `selectors` or `roles` field in src/acl.c must be a KNOWN,
# justified writer. A published (USER_FLAG_BOUND) user's rule set is reachable from an IO
# thread during admission, so it may only be REPLACED by the atomic-swap + epoch-bump +
# quiesce path (atomic_store_explicit ... memory_order_release). Every other writer must
# provably operate on an unpublished / staging / fresh object. A new mutation site that is
# not on this allowlist fails this check at review time -- before a probe happens to race it.
#
# Run: tests/acl-offload-source-guard.sh   (exit 0 = clean, 1 = unjustified writer found)
set -u
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
src="$here/../src/acl.c"
[ -f "$src" ] || { echo "acl.c not found at $src"; exit 2; }

# Justified writers: "<function>|<exact trimmed statement>". Published-user replacement is
# recognized separately by its atomic_store_explicit form and is never listed here.
allow() { printf '%s\n' "$1"; }
ALLOWED="$(cat <<'EOF'
ACLCreateUser|u->selectors = listCreate();
ACLCreateUser|u->roles = listCreate();
ACLUserClearRoles|u->roles = NULL;
ACLCreateRole|r->roles = NULL;
ACLCreateRole|r->selectors = listCreate();
ACLCopyUser|dst->roles = listCreate();
ACLCopyUser|dst->roles = NULL;
ACLSetUserRoles|u->roles = listCreate();
ACLStringSetRole|tempr->roles = NULL;
ACLStringSetRole|tempr->selectors = listCreate();
ACLStringSetRole|tempr->selectors = listDup(r->selectors);
ACLSetUser|u->roles = listCreate();
EOF
)"

violations=0
curfn=""
lineno=0
while IFS= read -r line; do
  lineno=$((lineno+1))
  # Function-definition header: starts at column 0 with a letter and names an id before '('.
  case "$line" in
    [A-Za-z_]*)
      if printf '%s' "$line" | grep -qE '[A-Za-z_][A-Za-z0-9_]*[[:space:]]*\('; then
        curfn="$(printf '%s' "$line" | grep -oE '[A-Za-z_][A-Za-z0-9_]*[[:space:]]*\(' | head -1 | sed -E 's/[[:space:]]*\(//')"
      fi
      ;;
  esac
  # A store to selectors/roles?
  if printf '%s' "$line" | grep -qE '(->selectors|->roles)[[:space:]]*='; then
    # Published-safe atomic swap is always OK.
    if printf '%s' "$line" | grep -q 'atomic_store_explicit'; then continue; fi
    stmt="$(printf '%s' "$line" | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//')"
    key="$curfn|$stmt"
    if ! printf '%s\n' "$ALLOWED" | grep -qxF "$key"; then
      echo "UNJUSTIFIED: line $lineno in $curfn: $stmt"
      violations=$((violations+1))
    fi
  fi
done < "$src"

if [ "$violations" -ne 0 ]; then
  echo "FAIL: $violations unjustified selectors/roles writer(s)."
  echo "Give the site the atomic swap+epoch-bump+quiesce discipline, or add it to the"
  echo "allowlist in $(basename "${BASH_SOURCE[0]}") with a justification if it provably"
  echo "operates on an unpublished/staging/fresh user object."
  exit 1
fi
echo "OK: all selectors/roles writers in src/acl.c are justified."
exit 0
