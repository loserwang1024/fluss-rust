// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Per-schema `ReadContext` cache for schema evolution support.
//!
//! In DYNAMIC mode (no projection), records are returned with their write-time
//! schema: old-schema batches return fewer columns, new-schema batches return
//! more columns.
//!
//! When projection is active, the schema is pinned at scanner creation time
//! and all batches use the initial ReadContext regardless of schema_id.

use crate::error::Result;
use crate::metadata::Schema;
use crate::record::{ReadContext, to_arrow_schema};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Resolves `ReadContext` per schema version to support schema evolution.
pub(crate) struct ReadContextResolver {
    /// Schema ID at scanner creation time.
    initial_schema_id: i16,
    /// ReadContexts keyed by schema_id. Contains both local and remote contexts.
    contexts: RwLock<HashMap<i16, ResolvedContexts>>,
    /// When Some, projection is active and schema is pinned to the initial one.
    projected_fields: Option<Vec<usize>>,
}

/// A pair of ReadContexts for local and remote reads.
struct ResolvedContexts {
    local: ReadContext,
    remote: ReadContext,
}

impl ReadContextResolver {
    /// Create a new resolver with the initial schema's ReadContexts.
    pub fn new(
        initial_schema_id: i16,
        local_context: ReadContext,
        remote_context: ReadContext,
        projected_fields: Option<Vec<usize>>,
    ) -> Self {
        let mut map = HashMap::new();
        map.insert(
            initial_schema_id,
            ResolvedContexts {
                local: local_context,
                remote: remote_context,
            },
        );
        Self {
            initial_schema_id,
            contexts: RwLock::new(map),
            projected_fields,
        }
    }

    /// Resolve the ReadContext for the given schema_id.
    /// Returns the initial context if projection is active (schema pinned).
    /// Returns None if the schema_id is not yet cached.
    pub fn resolve(&self, schema_id: i16, is_remote: bool) -> Option<ReadContext> {
        // If projection is active, always return the initial context
        let effective_id = if self.projected_fields.is_some() {
            self.initial_schema_id
        } else {
            schema_id
        };

        let guard = self.contexts.read();
        guard.get(&effective_id).map(|ctx| {
            if is_remote {
                ctx.remote.clone()
            } else {
                ctx.local.clone()
            }
        })
    }

    /// Check if a schema_id is already cached.
    pub fn contains(&self, schema_id: i16) -> bool {
        if self.projected_fields.is_some() {
            // projection pinned, always have the answer
            true
        } else {
            self.contexts.read().contains_key(&schema_id)
        }
    }

    /// Register a new schema by its ID. Builds ReadContexts from the Schema.
    /// No-op if already cached or if projection is active.
    pub fn register_schema(&self, schema_id: i16, schema: &Schema) -> Result<()> {
        if self.projected_fields.is_some() {
            // Projection pins the schema, no need to register new ones
            return Ok(());
        }
        if self.contexts.read().contains_key(&schema_id) {
            return Ok(());
        }

        let row_type = schema.row_type();
        let arrow_schema = to_arrow_schema(row_type)?;
        let row_type_arc = Arc::new(row_type.clone());

        let local_context = ReadContext::new(arrow_schema.clone(), row_type_arc.clone(), false)
            .with_fluss_row_type(row_type_arc.clone());
        let remote_context = ReadContext::new(arrow_schema, row_type_arc.clone(), true)
            .with_fluss_row_type(row_type_arc);

        self.contexts.write().insert(
            schema_id,
            ResolvedContexts {
                local: local_context,
                remote: remote_context,
            },
        );
        Ok(())
    }

    /// Returns the initial schema ID.
    pub fn initial_schema_id(&self) -> i16 {
        self.initial_schema_id
    }

    /// Returns the projection fields if active, used for fetch request pushdown.
    #[allow(dead_code)]
    pub fn projected_fields(&self) -> Option<&[usize]> {
        self.projected_fields.as_deref()
    }
}

/// Extract all unique schema_ids from raw log record batch bytes.
///
/// Scans through the concatenated batch buffer reading each batch header
/// to extract the schema_id field. Used to pre-resolve schemas asynchronously
/// before synchronous record decoding.
pub(crate) fn extract_schema_ids(data: &[u8]) -> Vec<i16> {
    use crate::record::{LENGTH_OFFSET, LOG_OVERHEAD, SCHEMA_ID_OFFSET};
    use byteorder::{ByteOrder, LittleEndian};

    let mut schema_ids = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut pos = 0;

    while pos + LOG_OVERHEAD <= data.len() {
        // Read batch length at LENGTH_OFFSET within the current batch
        let length_pos = pos + LENGTH_OFFSET;
        if length_pos + 4 > data.len() {
            break;
        }
        let batch_size_bytes = LittleEndian::read_i32(&data[length_pos..length_pos + 4]);
        if batch_size_bytes < 0 {
            break;
        }
        let batch_total_size = batch_size_bytes as usize + LOG_OVERHEAD;

        // Read schema_id
        let schema_id_pos = pos + SCHEMA_ID_OFFSET;
        if schema_id_pos + 2 > data.len() {
            break;
        }
        let schema_id = LittleEndian::read_i16(&data[schema_id_pos..schema_id_pos + 2]);
        if seen.insert(schema_id) {
            schema_ids.push(schema_id);
        }

        // Advance to next batch
        pos += batch_total_size;
    }

    schema_ids
}
