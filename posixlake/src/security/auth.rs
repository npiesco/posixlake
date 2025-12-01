//! Authentication module with bcrypt password hashing

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// User credentials
pub type Credentials = Option<(&'static str, &'static str)>;

/// Authentication context for a database session
#[derive(Debug, Clone)]
pub struct AuthContext {
    pub username: String,
    pub roles: Vec<String>,
    pub authenticated: bool,
}

impl AuthContext {
    /// Create an unauthenticated context (system access)
    pub fn system() -> Self {
        Self {
            username: "system".to_string(),
            roles: vec!["admin".to_string()],
            authenticated: true,
        }
    }

    /// Create an authenticated context for a user
    pub fn authenticated(username: String, roles: Vec<String>) -> Self {
        Self {
            username,
            roles,
            authenticated: true,
        }
    }

    /// Check if user has a specific role
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|r| r == role)
    }
}

/// User account
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub username: String,
    pub password_hash: String,
    pub roles: Vec<String>,
    pub created_at: i64,
}

impl User {
    /// Create a new user with bcrypt hashed password
    pub fn new(username: String, password: &str, roles: Vec<String>) -> Result<Self> {
        let password_hash = bcrypt::hash(password, bcrypt::DEFAULT_COST)
            .map_err(|e| Error::Other(format!("Failed to hash password: {}", e)))?;

        Ok(Self {
            username,
            password_hash,
            roles,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        })
    }

    /// Verify password against hash
    pub fn verify_password(&self, password: &str) -> bool {
        bcrypt::verify(password, &self.password_hash).unwrap_or(false)
    }
}

/// User store for persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserStore {
    users: HashMap<String, User>,
}

impl Default for UserStore {
    fn default() -> Self {
        Self::new()
    }
}

impl UserStore {
    /// Create a new empty user store
    pub fn new() -> Self {
        Self {
            users: HashMap::new(),
        }
    }

    /// Load user store from disk
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Self::new());
        }

        let content = std::fs::read_to_string(path)?;
        let store: UserStore = serde_json::from_str(&content)?;
        Ok(store)
    }

    /// Save user store to disk
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref();

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let json = serde_json::to_string_pretty(self)?;

        // Atomic write: .tmp → rename
        let tmp_path = path.with_extension("tmp");
        std::fs::write(&tmp_path, json)?;
        std::fs::rename(tmp_path, path)?;

        Ok(())
    }

    /// Add a new user
    pub fn add_user(&mut self, user: User) -> Result<()> {
        if self.users.contains_key(&user.username) {
            return Err(Error::Other(format!(
                "User already exists: {}",
                user.username
            )));
        }

        self.users.insert(user.username.clone(), user);
        Ok(())
    }

    /// Get a user by username
    pub fn get_user(&self, username: &str) -> Option<&User> {
        self.users.get(username)
    }

    /// Authenticate a user with credentials
    pub fn authenticate(&self, username: &str, password: &str) -> Result<AuthContext> {
        let user = self
            .get_user(username)
            .ok_or_else(|| Error::Other("Invalid credentials".to_string()))?;

        if !user.verify_password(password) {
            return Err(Error::Other("Invalid credentials".to_string()));
        }

        Ok(AuthContext::authenticated(
            user.username.clone(),
            user.roles.clone(),
        ))
    }

    /// Remove a role from a user
    pub fn revoke_role(&mut self, username: &str, role: &str) -> Result<()> {
        let user = self
            .users
            .get_mut(username)
            .ok_or_else(|| Error::Other(format!("User not found: {}", username)))?;

        user.roles.retain(|r| r != role);
        Ok(())
    }

    /// Add a role to a user
    pub fn grant_role(&mut self, username: &str, role: &str) -> Result<()> {
        let user = self
            .users
            .get_mut(username)
            .ok_or_else(|| Error::Other(format!("User not found: {}", username)))?;

        if !user.roles.contains(&role.to_string()) {
            user.roles.push(role.to_string());
        }
        Ok(())
    }
}

impl User {
    /// Check if user has a specific role
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|r| r == role)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_password_hashing() {
        let user = User::new("test".to_string(), "password123", vec!["read".to_string()]).unwrap();

        // Password should be hashed
        assert_ne!(user.password_hash, "password123");
        assert!(user.password_hash.starts_with("$2")); // bcrypt marker

        // Correct password verifies
        assert!(user.verify_password("password123"));

        // Wrong password fails
        assert!(!user.verify_password("wrongpassword"));
    }

    #[test]
    fn test_user_store() {
        let mut store = UserStore::new();

        let user = User::new("alice".to_string(), "secret", vec!["admin".to_string()]).unwrap();
        store.add_user(user).unwrap();

        // Can retrieve user
        let retrieved = store.get_user("alice").unwrap();
        assert_eq!(retrieved.username, "alice");
        assert!(retrieved.has_role("admin"));

        // Can authenticate
        let auth_ctx = store.authenticate("alice", "secret").unwrap();
        assert_eq!(auth_ctx.username, "alice");
        assert!(auth_ctx.has_role("admin"));

        // Wrong password fails
        assert!(store.authenticate("alice", "wrong").is_err());

        // Non-existent user fails
        assert!(store.authenticate("bob", "password").is_err());
    }

    #[test]
    fn test_role_management() {
        let mut store = UserStore::new();
        let user = User::new("bob".to_string(), "pass", vec!["read".to_string()]).unwrap();
        store.add_user(user).unwrap();

        // User has read role
        let user = store.get_user("bob").unwrap();
        assert!(user.has_role("read"));
        assert!(!user.has_role("write"));

        // Grant write role
        store.grant_role("bob", "write").unwrap();
        let user = store.get_user("bob").unwrap();
        assert!(user.has_role("write"));

        // Revoke read role
        store.revoke_role("bob", "read").unwrap();
        let user = store.get_user("bob").unwrap();
        assert!(!user.has_role("read"));
        assert!(user.has_role("write"));
    }
}
