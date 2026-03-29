//! Database repository pattern for clean data access.
//!
//! This module provides:
//! - Generic repository trait for CRUD operations
//! - Pagination and filtering support
//! - Transaction management helpers

use async_trait::async_trait;
use serde::Serialize;
use sqlx::PgPool;
use uuid::Uuid;

use crate::core::error::ApiError;

/// Generic repository trait for database operations
#[async_trait]
pub trait Repository<T, CreateDto, UpdateDto>: Send + Sync
where
    T: Send + Sync,
    CreateDto: Send + Sync,
    UpdateDto: Send + Sync,
{
    /// Find entity by ID
    async fn find_by_id(&self, id: Uuid) -> Result<Option<T>, ApiError>;

    /// Find all entities with pagination
    async fn find_all(&self, pagination: &Pagination) -> Result<PagedResult<T>, ApiError>;

    /// Create a new entity
    async fn create(&self, dto: CreateDto) -> crate::core::error::Result<T>;

    /// Update an existing entity
    async fn update(&self, id: Uuid, dto: UpdateDto) -> crate::core::error::Result<T>;

    /// Delete an entity by ID
    async fn delete(&self, id: Uuid) -> crate::core::error::Result<bool>;

    /// Check if entity exists
    async fn exists(&self, id: Uuid) -> crate::core::error::Result<bool>;

    /// Count all entities
    async fn count(&self) -> crate::core::error::Result<i64>;
}

/// Pagination parameters
#[derive(Debug, Clone)]
pub struct Pagination {
    pub page: u32,
    pub per_page: u32,
    pub sort_by: Option<String>,
    pub sort_order: SortOrder,
}

impl Default for Pagination {
    fn default() -> Self {
        Self {
            page: 1,
            per_page: 20,
            sort_by: None,
            sort_order: SortOrder::Asc,
        }
    }
}

impl Pagination {
    pub fn new(page: u32, per_page: u32) -> Self {
        Self {
            page: page.max(1),
            per_page: per_page.clamp(1, 100),
            ..Default::default()
        }
    }

    pub fn with_sort(mut self, column: impl Into<String>, order: SortOrder) -> Self {
        self.sort_by = Some(column.into());
        self.sort_order = order;
        self
    }

    pub fn offset(&self) -> i64 {
        ((self.page - 1) * self.per_page) as i64
    }

    pub fn limit(&self) -> i64 {
        self.per_page as i64
    }
}

/// Sort order for queries
#[derive(Debug, Clone, Copy, Default)]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

impl SortOrder {
    pub fn as_str(&self) -> &'static str {
        match self {
            SortOrder::Asc => "ASC",
            SortOrder::Desc => "DESC",
        }
    }
}

/// Paged result containing items and metadata
#[derive(Debug, Clone, Serialize)]
pub struct PagedResult<T> {
    pub items: Vec<T>,
    pub total: i64,
    pub page: u32,
    pub per_page: u32,
    pub total_pages: u32,
}

impl<T> PagedResult<T> {
    pub fn new(items: Vec<T>, total: i64, pagination: &Pagination) -> Self {
        let total_pages = ((total as f64) / (pagination.per_page as f64)).ceil() as u32;
        Self {
            items,
            total,
            page: pagination.page,
            per_page: pagination.per_page,
            total_pages,
        }
    }

    pub fn empty(pagination: &Pagination) -> Self {
        Self {
            items: Vec::new(),
            total: 0,
            page: pagination.page,
            per_page: pagination.per_page,
            total_pages: 0,
        }
    }

    pub fn has_next_page(&self) -> bool {
        self.page < self.total_pages
    }

    pub fn has_prev_page(&self) -> bool {
        self.page > 1
    }

    pub fn map<U, F>(self, f: F) -> PagedResult<U>
    where
        F: FnMut(T) -> U,
    {
        PagedResult {
            items: self.items.into_iter().map(f).collect(),
            total: self.total,
            page: self.page,
            per_page: self.per_page,
            total_pages: self.total_pages,
        }
    }
}

/// Query filter builder for dynamic queries
#[derive(Debug, Clone, Default)]
pub struct QueryFilter {
    conditions: Vec<FilterCondition>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct FilterCondition {
    field: String,
    operator: FilterOperator,
    value: FilterValue,
}

#[derive(Debug, Clone)]
enum FilterOperator {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Like,
    #[allow(dead_code)]
    In,
    IsNull,
    IsNotNull,
}

/// Filter value types for query building
#[derive(Debug, Clone)]
pub enum FilterValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Uuid(Uuid),
    #[allow(dead_code)]
    StringList(Vec<String>),
    Null,
}

impl QueryFilter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn eq(mut self, field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        self.conditions.push(FilterCondition {
            field: field.into(),
            operator: FilterOperator::Eq,
            value: value.into(),
        });
        self
    }

    pub fn ne(mut self, field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        self.conditions.push(FilterCondition {
            field: field.into(),
            operator: FilterOperator::Ne,
            value: value.into(),
        });
        self
    }

    pub fn gt(mut self, field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        self.conditions.push(FilterCondition {
            field: field.into(),
            operator: FilterOperator::Gt,
            value: value.into(),
        });
        self
    }

    pub fn gte(mut self, field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        self.conditions.push(FilterCondition {
            field: field.into(),
            operator: FilterOperator::Gte,
            value: value.into(),
        });
        self
    }

    pub fn lt(mut self, field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        self.conditions.push(FilterCondition {
            field: field.into(),
            operator: FilterOperator::Lt,
            value: value.into(),
        });
        self
    }

    pub fn lte(mut self, field: impl Into<String>, value: impl Into<FilterValue>) -> Self {
        self.conditions.push(FilterCondition {
            field: field.into(),
            operator: FilterOperator::Lte,
            value: value.into(),
        });
        self
    }

    pub fn like(mut self, field: impl Into<String>, pattern: impl Into<String>) -> Self {
        self.conditions.push(FilterCondition {
            field: field.into(),
            operator: FilterOperator::Like,
            value: FilterValue::String(pattern.into()),
        });
        self
    }

    pub fn is_null(mut self, field: impl Into<String>) -> Self {
        self.conditions.push(FilterCondition {
            field: field.into(),
            operator: FilterOperator::IsNull,
            value: FilterValue::Null,
        });
        self
    }

    pub fn is_not_null(mut self, field: impl Into<String>) -> Self {
        self.conditions.push(FilterCondition {
            field: field.into(),
            operator: FilterOperator::IsNotNull,
            value: FilterValue::Null,
        });
        self
    }

    pub fn is_empty(&self) -> bool {
        self.conditions.is_empty()
    }

    pub fn len(&self) -> usize {
        self.conditions.len()
    }
}

// Implement From traits for FilterValue
impl From<String> for FilterValue {
    fn from(v: String) -> Self {
        FilterValue::String(v)
    }
}

impl From<&str> for FilterValue {
    fn from(v: &str) -> Self {
        FilterValue::String(v.to_string())
    }
}

impl From<i64> for FilterValue {
    fn from(v: i64) -> Self {
        FilterValue::Int(v)
    }
}

impl From<i32> for FilterValue {
    fn from(v: i32) -> Self {
        FilterValue::Int(v as i64)
    }
}

impl From<f64> for FilterValue {
    fn from(v: f64) -> Self {
        FilterValue::Float(v)
    }
}

impl From<bool> for FilterValue {
    fn from(v: bool) -> Self {
        FilterValue::Bool(v)
    }
}

impl From<Uuid> for FilterValue {
    fn from(v: Uuid) -> Self {
        FilterValue::Uuid(v)
    }
}

/// Transaction wrapper for database operations
pub struct Transaction<'a> {
    tx: sqlx::Transaction<'a, sqlx::Postgres>,
}

impl<'a> Transaction<'a> {
    pub async fn begin(pool: &'a PgPool) -> crate::core::error::Result<Self> {
        let tx = pool.begin().await.map_err(ApiError::from)?;
        Ok(Self { tx })
    }

    pub async fn commit(self) -> crate::core::error::Result<()> {
        self.tx.commit().await.map_err(ApiError::from)
    }

    pub async fn rollback(self) -> crate::core::error::Result<()> {
        self.tx.rollback().await.map_err(ApiError::from)
    }

    pub fn inner(&mut self) -> &mut sqlx::Transaction<'a, sqlx::Postgres> {
        &mut self.tx
    }
}

/// Helper macro for running operations in a transaction
#[macro_export]
macro_rules! with_transaction {
    ($pool:expr, $tx:ident, $body:block) => {{
        let mut $tx = $crate::infra::db::repository::Transaction::begin($pool).await?;
        let result = async { $body }.await;
        match result {
            Ok(value) => {
                $tx.commit().await?;
                Ok(value)
            }
            Err(e) => {
                let _ = $tx.rollback().await;
                Err(e)
            }
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pagination_defaults() {
        let pagination = Pagination::default();
        assert_eq!(pagination.page, 1);
        assert_eq!(pagination.per_page, 20);
        assert_eq!(pagination.offset(), 0);
        assert_eq!(pagination.limit(), 20);
    }

    #[test]
    fn test_pagination_offset() {
        let pagination = Pagination::new(3, 10);
        assert_eq!(pagination.offset(), 20);
    }

    #[test]
    fn test_pagination_clamp() {
        let pagination = Pagination::new(0, 200);
        assert_eq!(pagination.page, 1);
        assert_eq!(pagination.per_page, 100);
    }

    #[test]
    fn test_paged_result() {
        let pagination = Pagination::new(2, 10);
        let result: PagedResult<i32> = PagedResult::new(vec![1, 2, 3], 25, &pagination);

        assert_eq!(result.total, 25);
        assert_eq!(result.page, 2);
        assert_eq!(result.total_pages, 3);
        assert!(result.has_next_page());
        assert!(result.has_prev_page());
    }

    #[test]
    fn test_query_filter() {
        let filter = QueryFilter::new()
            .eq("status", "active")
            .gt("amount", 100i64)
            .is_not_null("email");

        assert_eq!(filter.len(), 3);
        assert!(!filter.is_empty());
    }
}
