//! Data import utilities for creating Delta Lake tables from external formats
//!
//! This module provides functions to import data from CSV and Parquet files
//! into Delta Lake format with automatic schema inference.

mod csv;
mod parquet;

pub use csv::create_from_csv;
pub use parquet::create_from_parquet;
