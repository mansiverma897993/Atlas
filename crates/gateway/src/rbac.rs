//! Route classification and RBAC permission matching (ARCHITECTURE §6.3).
//!
//! A single static table maps each `(method, path-template)` to:
//!
//! * a **stable route label** for RED metrics (keeps cardinality bounded — real ids like
//!   `/api/accounts/123` collapse to the template `/api/accounts/:id`), and
//! * the **permission** a caller must hold to invoke it (`None` for public routes such as
//!   `/api/auth/*` and health).
//!
//! Both the metrics layer and the RBAC layer classify the *actual* request path against this
//! table, so neither depends on Axum's `MatchedPath` (which isn't visible to top-level
//! middleware). The matcher and the permission check are pure and unit-tested.

use crate::auth::Claims;

/// The classification of a request against the known route table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteClass {
    /// Stable template label (e.g. `/api/accounts/:id`) used for metrics.
    pub template: &'static str,
    /// Permission required to call this route, if any.
    pub permission: Option<&'static str>,
}

/// `(HTTP method, path template, required permission)`.
///
/// Path templates use `:seg` to match exactly one non-empty path segment. Order matters only
/// in that the first match wins; templates here are unambiguous.
const ROUTES: &[(&str, &str, Option<&str>)] = &[
    // ---- public auth surface (no permission) ----
    ("POST", "/api/auth/register", None),
    ("POST", "/api/auth/login", None),
    ("POST", "/api/auth/refresh", None),
    ("POST", "/api/auth/logout", None),
    // ---- ledger surface (RBAC-protected) ----
    ("POST", "/api/accounts", Some("ledger:account:open")),
    ("GET", "/api/accounts/:id", Some("ledger:account:read")),
    (
        "GET",
        "/api/accounts/:id/balance",
        Some("ledger:account:read"),
    ),
    (
        "GET",
        "/api/accounts/:id/transactions",
        Some("ledger:account:read"),
    ),
    ("POST", "/api/transfers", Some("ledger:transfer:create")),
    ("GET", "/api/transfers/:id", Some("ledger:transfer:read")),
    // ---- health (no permission; labelled for clean metrics) ----
    ("GET", "/health/live", None),
    ("GET", "/health/ready", None),
    ("GET", "/health/startup", None),
];

/// Classify a request against the known routes. Returns `None` for unknown paths.
#[must_use]
pub fn classify(method: &str, path: &str) -> Option<RouteClass> {
    ROUTES.iter().find_map(|(m, template, permission)| {
        if m.eq_ignore_ascii_case(method) && path_matches(template, path) {
            Some(RouteClass {
                template,
                permission: *permission,
            })
        } else {
            None
        }
    })
}

/// A bounded-cardinality metrics label for a request (the template, or `unknown`).
#[must_use]
pub fn route_label(method: &str, path: &str) -> &'static str {
    classify(method, path).map_or("unknown", |c| c.template)
}

/// Does `path` match `template`, where a `:seg` template segment matches any one segment?
#[must_use]
pub fn path_matches(template: &str, path: &str) -> bool {
    let mut t = template.split('/');
    let mut p = path.trim_end_matches('/').split('/');
    loop {
        match (t.next(), p.next()) {
            (Some(ts), Some(ps)) => {
                if let Some(_name) = ts.strip_prefix(':') {
                    if ps.is_empty() {
                        return false; // a param must capture a non-empty segment
                    }
                } else if ts != ps {
                    return false;
                }
            }
            (None, None) => return true,
            _ => return false, // different number of segments
        }
    }
}

/// Does a set of granted permissions satisfy a required one?
///
/// Supports exact grants, a global `*` super-grant, and colon-scoped wildcards such as
/// `ledger:*` or `ledger:account:*` (a grant ending in `:*` matches any required permission
/// sharing its prefix).
#[must_use]
pub fn permission_matches(granted: &str, required: &str) -> bool {
    if granted == required || granted == "*" {
        return true;
    }
    if let Some(prefix) = granted.strip_suffix(":*") {
        // `ledger:*` grants `ledger:account:read`, `ledger:transfer:create`, ...
        return required
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with(':'));
    }
    false
}

/// Does the caller hold the required permission?
#[must_use]
pub fn has_permission(claims: &Claims, required: &str) -> bool {
    claims
        .permissions
        .iter()
        .any(|granted| permission_matches(granted, required))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims_with(perms: &[&str]) -> Claims {
        Claims {
            sub: "user-1".into(),
            roles: vec![],
            permissions: perms.iter().map(|s| (*s).to_string()).collect(),
            exp: 0,
            iss: None,
        }
    }

    #[test]
    fn matches_templates_with_params() {
        assert!(path_matches("/api/accounts/:id", "/api/accounts/abc"));
        assert!(path_matches(
            "/api/accounts/:id/balance",
            "/api/accounts/abc/balance"
        ));
        assert!(!path_matches("/api/accounts/:id", "/api/accounts"));
        assert!(!path_matches(
            "/api/accounts/:id",
            "/api/accounts/abc/balance"
        ));
        assert!(!path_matches("/api/accounts/:id", "/api/transfers/abc"));
    }

    #[test]
    fn classify_resolves_permission_and_label() {
        let c = classify("GET", "/api/accounts/xyz/balance").unwrap();
        assert_eq!(c.template, "/api/accounts/:id/balance");
        assert_eq!(c.permission, Some("ledger:account:read"));

        let open = classify("POST", "/api/accounts").unwrap();
        assert_eq!(open.permission, Some("ledger:account:open"));

        let auth = classify("POST", "/api/auth/login").unwrap();
        assert_eq!(auth.permission, None);

        assert!(classify("GET", "/nope").is_none());
        assert_eq!(route_label("GET", "/nope"), "unknown");
    }

    #[test]
    fn method_is_matched() {
        // GET /api/transfers is not a registered route (only POST is).
        assert!(classify("GET", "/api/transfers").is_none());
        assert!(classify("POST", "/api/transfers").is_some());
    }

    #[test]
    fn exact_permission_grants_access() {
        let claims = claims_with(&["ledger:account:read"]);
        assert!(has_permission(&claims, "ledger:account:read"));
        assert!(!has_permission(&claims, "ledger:transfer:create"));
    }

    #[test]
    fn wildcard_grants() {
        assert!(permission_matches("*", "ledger:account:read"));
        assert!(permission_matches("ledger:*", "ledger:account:read"));
        assert!(permission_matches(
            "ledger:account:*",
            "ledger:account:read"
        ));
        assert!(!permission_matches(
            "ledger:account:*",
            "ledger:transfer:create"
        ));
        // prefix must be colon-bounded: `ledger:acc*` must not match `ledger:account`
        assert!(!permission_matches("identity:*", "ledger:account:read"));
    }

    #[test]
    fn has_permission_respects_scoped_wildcard() {
        let claims = claims_with(&["ledger:*"]);
        assert!(has_permission(&claims, "ledger:transfer:create"));
        assert!(!has_permission(&claims, "identity:user:read"));
    }
}
