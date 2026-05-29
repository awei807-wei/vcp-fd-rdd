//! Local transport and path-boundary security helpers.

use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpPolicy {
    Disabled,
    LocalhostDebug,
    Token,
    Unsafe,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RunningIdentity {
    pub uid: u32,
    pub system_daemon: bool,
}

impl RunningIdentity {
    pub fn current() -> Self {
        #[cfg(unix)]
        {
            Self {
                uid: unsafe { libc::geteuid() },
                system_daemon: false,
            }
        }
        #[cfg(not(unix))]
        {
            Self {
                uid: 0,
                system_daemon: false,
            }
        }
    }

    pub fn multi_user_risk(self) -> bool {
        self.uid == 0 || self.system_daemon
    }
}

pub fn effective_http_policy(
    configured: Option<HttpPolicy>,
    identity: RunningIdentity,
) -> HttpPolicy {
    match configured {
        Some(policy) => policy,
        None if identity.multi_user_risk() => HttpPolicy::Disabled,
        None => HttpPolicy::LocalhostDebug,
    }
}

pub fn path_within_roots(path: &Path, roots: &[PathBuf]) -> bool {
    roots.iter().any(|root| path.starts_with(root))
}

pub fn filter_readable_overfetch<T>(
    candidates: impl IntoIterator<Item = T>,
    limit: usize,
    can_read: impl Fn(&T) -> bool,
) -> Vec<T> {
    let mut out = Vec::with_capacity(limit);
    for item in candidates {
        if can_read(&item) {
            out.push(item);
            if out.len() >= limit {
                break;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_identity_disables_http_by_default() {
        let policy = effective_http_policy(
            None,
            RunningIdentity {
                uid: 0,
                system_daemon: false,
            },
        );
        assert_eq!(policy, HttpPolicy::Disabled);
    }

    #[test]
    fn scan_path_must_stay_inside_configured_roots() {
        let roots = vec![PathBuf::from("/home/user/work")];
        assert!(path_within_roots(Path::new("/home/user/work/src"), &roots));
        assert!(!path_within_roots(Path::new("/etc"), &roots));
    }

    #[test]
    fn permission_filter_runs_before_limit() {
        let values = vec![1, 2, 3, 4, 5];
        let out = filter_readable_overfetch(values, 2, |v| *v >= 3);
        assert_eq!(out, vec![3, 4]);
    }
}
