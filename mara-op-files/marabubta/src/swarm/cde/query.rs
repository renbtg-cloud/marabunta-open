// Marabunta - Licensed under the MIT License.
use super::CdeError;


pub struct QueryEngine;

impl QueryEngine {
    pub async fn execute_query(&self, _query_str: &str) -> Result<(), CdeError> {
        Err(CdeError::SqlParse("SQL Support Removed. Use JCL.".into()))
    }
}
