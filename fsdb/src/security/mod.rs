//! Security & Access Control Module
//!
//! Provides enterprise-grade security features:
//! - User authentication with bcrypt password hashing
//! - Role-based access control (RBAC)
//! - Audit logging
//! - Permission enforcement

pub mod audit;
pub mod auth;
pub mod rbac;

pub use audit::{AuditEntry, AuditLog, AuditLogger};
pub use auth::{AuthContext, Credentials, User, UserStore};
pub use rbac::{Permission, Role, RoleManager};
