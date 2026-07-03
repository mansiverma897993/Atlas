//! RBAC value objects and the pure authorization decision. Roles group permissions; a user's
//! **effective permissions** are the union across their roles. Authorization is a set-membership
//! check with optional wildcard support, kept pure so it is trivially testable and identical
//! wherever it runs (gateway Tower layer and this service — defense in depth, ADR-0009).

/// A named role carrying the permissions it grants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Role {
    /// Role name, e.g. `"customer"`.
    pub name: String,
    /// Permission strings this role grants.
    pub permissions: Vec<String>,
}

/// Does `effective` satisfy the `required` permission?
///
/// Matching rules (evaluated in order):
/// * exact string match, or
/// * the superuser wildcard `"*"`, or
/// * a trailing-segment wildcard like `"ledger:*"` that matches any permission sharing the
///   prefix `"ledger:"` (colon-delimited namespaces).
///
/// **Pure.**
#[must_use]
pub fn authorize(effective: &[String], required: &str) -> bool {
    effective
        .iter()
        .any(|granted| permission_matches(granted, required))
}

/// Whether a single granted permission satisfies the required one.
fn permission_matches(granted: &str, required: &str) -> bool {
    if granted == required || granted == "*" {
        return true;
    }
    // Namespace wildcard: "ledger:*" matches "ledger:transfer:create".
    if let Some(prefix) = granted.strip_suffix('*') {
        // `prefix` includes the trailing separator, e.g. "ledger:".
        return required.starts_with(prefix);
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn perms(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn exact_match_authorizes() {
        let p = perms(&["ledger:account:read", "ledger:transfer:create"]);
        assert!(authorize(&p, "ledger:transfer:create"));
        assert!(!authorize(&p, "ledger:transfer:reverse"));
    }

    #[test]
    fn superuser_wildcard_authorizes_anything() {
        let p = perms(&["*"]);
        assert!(authorize(&p, "anything:at:all"));
    }

    #[test]
    fn namespace_wildcard_matches_prefix() {
        let p = perms(&["ledger:*"]);
        assert!(authorize(&p, "ledger:transfer:create"));
        assert!(authorize(&p, "ledger:account:read"));
        assert!(!authorize(&p, "identity:user:delete"));
    }

    #[test]
    fn empty_permissions_authorize_nothing() {
        assert!(!authorize(&[], "ledger:account:read"));
    }
}
