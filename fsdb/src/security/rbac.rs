//! Role-Based Access Control (RBAC)

use serde::{Deserialize, Serialize};

/// Permission types
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Permission {
    Read,
    Write,
    Delete,
    Admin,
    Backup,
    Restore,
}

/// Role definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    pub name: String,
    pub permissions: Vec<Permission>,
}

impl Role {
    /// Create a new role
    pub fn new(name: String, permissions: Vec<Permission>) -> Self {
        Self { name, permissions }
    }

    /// Check if role has a specific permission
    pub fn has_permission(&self, permission: &Permission) -> bool {
        self.permissions.contains(permission)
    }
}

/// Role manager
pub struct RoleManager {
    roles: std::collections::HashMap<String, Role>,
}

impl Default for RoleManager {
    fn default() -> Self {
        Self::new()
    }
}

impl RoleManager {
    /// Create a new role manager with default roles
    pub fn new() -> Self {
        let mut roles = std::collections::HashMap::new();

        // Admin role - all permissions
        roles.insert(
            "admin".to_string(),
            Role::new(
                "admin".to_string(),
                vec![
                    Permission::Read,
                    Permission::Write,
                    Permission::Delete,
                    Permission::Admin,
                    Permission::Backup,
                    Permission::Restore,
                ],
            ),
        );

        // Read role - only read permission
        roles.insert(
            "read".to_string(),
            Role::new("read".to_string(), vec![Permission::Read]),
        );

        // Write role - only write permission
        roles.insert(
            "write".to_string(),
            Role::new("write".to_string(), vec![Permission::Write]),
        );

        Self { roles }
    }

    /// Check if user roles have a specific permission
    pub fn has_permission(&self, user_roles: &[String], permission: &Permission) -> bool {
        user_roles.iter().any(|role_name| {
            self.roles
                .get(role_name)
                .map(|role| role.has_permission(permission))
                .unwrap_or(false)
        })
    }

    /// Get role by name
    pub fn get_role(&self, name: &str) -> Option<&Role> {
        self.roles.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_role_permissions() {
        let manager = RoleManager::new();

        // Admin has all permissions
        assert!(manager.has_permission(&["admin".to_string()], &Permission::Read));
        assert!(manager.has_permission(&["admin".to_string()], &Permission::Write));
        assert!(manager.has_permission(&["admin".to_string()], &Permission::Delete));
        assert!(manager.has_permission(&["admin".to_string()], &Permission::Admin));

        // Read role only has read permission
        assert!(manager.has_permission(&["read".to_string()], &Permission::Read));
        assert!(!manager.has_permission(&["read".to_string()], &Permission::Write));
        assert!(!manager.has_permission(&["read".to_string()], &Permission::Delete));

        // Write role only has write permission
        assert!(!manager.has_permission(&["write".to_string()], &Permission::Read));
        assert!(manager.has_permission(&["write".to_string()], &Permission::Write));
        assert!(!manager.has_permission(&["write".to_string()], &Permission::Delete));

        // Multiple roles combine permissions
        assert!(manager.has_permission(
            &["read".to_string(), "write".to_string()],
            &Permission::Read
        ));
        assert!(manager.has_permission(
            &["read".to_string(), "write".to_string()],
            &Permission::Write
        ));
        assert!(!manager.has_permission(
            &["read".to_string(), "write".to_string()],
            &Permission::Delete
        ));
    }
}
