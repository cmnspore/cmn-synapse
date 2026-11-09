use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::RwLock;

use anyhow::Result;
use ruvector_core::DistanceMetric;
use ruvector_core::advanced::hypergraph::TemporalGranularity;
use ruvector_core::advanced::{Hyperedge, HypergraphIndex, TemporalHyperedge};
use serde::Serialize;

use super::EmbedClient;
use crate::storage::{LineageNode, LineageRef, SporeBond};

/// Graph statistics.
#[derive(Debug, Serialize)]
pub struct GraphStats {
    pub total_nodes: usize,
    pub total_edges: usize,
    pub avg_degree: f32,
}

/// In-memory spore evolution graph backed by ruvector-core HypergraphIndex.
///
/// Uses HypergraphIndex for semantic edge search and temporal queries,
/// plus separate directed adjacency maps for BFS lineage traversal
/// (since HypergraphIndex::k_hop_neighbors is undirected).
pub struct SporeGraph {
    index: RwLock<HypergraphIndex>,
    /// parent_hash → set of child_hashes (descendants)
    forward_edges: RwLock<HashMap<String, HashSet<String>>>,
    /// child_hash → vec of (parent_uri, relation)
    reverse_edges: RwLock<HashMap<String, Vec<(String, substrate::BondRelation)>>>,
    /// hash → primary URI (first domain that published it)
    node_uris: RwLock<HashMap<String, String>>,
    /// Optional embedder for relationship edge descriptions
    embedder: Option<EmbedClient>,
}

/// Acquire a RwLock for reading, converting a poisoned lock into an anyhow error.
fn read_lock<T>(lock: &RwLock<T>) -> Result<std::sync::RwLockReadGuard<'_, T>> {
    lock.read()
        .map_err(|_| anyhow::anyhow!("RwLock poisoned (read)"))
}

/// Acquire a RwLock for writing, converting a poisoned lock into an anyhow error.
fn write_lock<T>(lock: &RwLock<T>) -> Result<std::sync::RwLockWriteGuard<'_, T>> {
    lock.write()
        .map_err(|_| anyhow::anyhow!("RwLock poisoned (write)"))
}

impl SporeGraph {
    pub fn new(embedder: Option<EmbedClient>) -> Self {
        Self {
            index: RwLock::new(HypergraphIndex::new(DistanceMetric::Cosine)),
            forward_edges: RwLock::new(HashMap::new()),
            reverse_edges: RwLock::new(HashMap::new()),
            node_uris: RwLock::new(HashMap::new()),
            embedder,
        }
    }

    /// Register a spore node. Stores its URI for lineage reconstruction.
    pub fn add_spore(&self, hash: &str, domain: &str) -> Result<()> {
        let uri = format!("cmn://{}/{}", domain, hash);
        let mut uris = write_lock(&self.node_uris)?;
        uris.entry(hash.to_string()).or_insert(uri);
        Ok(())
    }

    /// Add relationship edges from a child spore to its bonds.
    ///
    /// Each SporeBond becomes:
    ///   - A directed edge in forward/reverse adjacency maps (for BFS)
    ///   - A TemporalHyperedge in the HypergraphIndex (for semantic/temporal queries)
    pub async fn add_relationship(&self, child_hash: &str, bonds: &[SporeBond]) -> Result<()> {
        for reference in bonds {
            let parent_hash = parse_hash_from_uri(&reference.uri);

            // Update directed adjacency maps
            {
                let mut forward = write_lock(&self.forward_edges)?;
                forward
                    .entry(parent_hash.clone())
                    .or_default()
                    .insert(child_hash.to_string());
            }
            {
                let mut reverse = write_lock(&self.reverse_edges)?;
                reverse
                    .entry(child_hash.to_string())
                    .or_default()
                    .push((reference.uri.clone(), reference.relation.clone()));
            }

            // Build hyperedge for HypergraphIndex
            let edge_id = format!("{}->{}:{}", child_hash, parent_hash, reference.relation);
            let description = format!("{} {} {}", child_hash, reference.relation, parent_hash);

            let nodes = vec![child_hash.to_string(), parent_hash.clone()];

            let embedding = if let Some(ref embedder) = self.embedder {
                match embedder.embed(&[&description]).await {
                    Ok(mut embs) if !embs.is_empty() => embs.remove(0),
                    _ => vec![],
                }
            } else {
                vec![]
            };

            let mut metadata = HashMap::new();
            metadata.insert("relation".to_string(), reference.relation.to_string());
            metadata.insert("child".to_string(), child_hash.to_string());
            metadata.insert("parent".to_string(), parent_hash.clone());
            metadata.insert("parent_uri".to_string(), reference.uri.clone());

            let hyperedge = Hyperedge {
                id: edge_id,
                nodes,
                description,
                embedding,
                confidence: 1.0,
                metadata,
            };

            let temporal = TemporalHyperedge::new(hyperedge, TemporalGranularity::Daily);

            let mut index = write_lock(&self.index)?;
            let _ = index.add_temporal_hyperedge(temporal);
        }

        Ok(())
    }

    /// Get descendants via directed BFS on forward_edges.
    pub fn get_descendants(
        &self,
        hash: &str,
        max_depth: usize,
    ) -> Result<(Vec<LineageNode>, bool)> {
        let forward = read_lock(&self.forward_edges)?;
        let reverse = read_lock(&self.reverse_edges)?;
        let uris = read_lock(&self.node_uris)?;

        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut results = Vec::new();
        let mut max_reached = false;

        queue.push_back((hash.to_string(), 0usize));
        visited.insert(hash.to_string());

        while let Some((current, depth)) = queue.pop_front() {
            if max_depth > 0 && depth >= max_depth {
                max_reached = true;
                continue;
            }
            if let Some(children) = forward.get(&current) {
                for child in children {
                    if visited.insert(child.clone()) {
                        let node = build_lineage_node(child, &uris, &reverse);
                        results.push(node);
                        queue.push_back((child.clone(), depth + 1));
                    }
                }
            }
        }

        Ok((results, max_reached))
    }

    /// Get ancestors via directed BFS on reverse_edges.
    pub fn get_ancestors(&self, hash: &str, max_depth: usize) -> Result<(Vec<LineageNode>, bool)> {
        let reverse = read_lock(&self.reverse_edges)?;
        let uris = read_lock(&self.node_uris)?;

        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut results = Vec::new();
        let mut max_reached = false;

        queue.push_back((hash.to_string(), 0usize));
        visited.insert(hash.to_string());

        while let Some((current, depth)) = queue.pop_front() {
            if max_depth > 0 && depth >= max_depth {
                max_reached = true;
                continue;
            }
            if let Some(refs) = reverse.get(&current) {
                for (parent_uri, _relation) in refs {
                    let parent_hash = parse_hash_from_uri(parent_uri);
                    if visited.insert(parent_hash.clone()) {
                        let node = build_lineage_node(&parent_hash, &uris, &reverse);
                        results.push(node);
                        queue.push_back((parent_hash, depth + 1));
                    }
                }
            }
        }

        Ok((results, max_reached))
    }

    /// Graph statistics.
    pub fn stats(&self) -> Result<GraphStats> {
        let forward = read_lock(&self.forward_edges)?;
        let reverse = read_lock(&self.reverse_edges)?;
        let index = read_lock(&self.index)?;
        let hg_stats = index.stats();

        let mut all_nodes = HashSet::new();
        for (parent, children) in forward.iter() {
            all_nodes.insert(parent.clone());
            for child in children {
                all_nodes.insert(child.clone());
            }
        }
        for child in reverse.keys() {
            all_nodes.insert(child.clone());
        }

        let total_nodes = all_nodes.len();
        let total_edges = hg_stats.total_hyperedges;
        let avg_degree = if total_nodes > 0 {
            (total_edges as f32 * 2.0) / total_nodes as f32
        } else {
            0.0
        };

        Ok(GraphStats {
            total_nodes,
            total_edges,
            avg_degree,
        })
    }
}

/// Extract hash from a CMN URI: "cmn://domain/hash" → "hash"
fn parse_hash_from_uri(uri: &str) -> String {
    if let Ok(parsed) = substrate::parse_uri(uri)
        && parsed.kind == substrate::CmnUriKind::Spore
        && let Some(hash) = parsed.hash
    {
        return hash;
    }
    uri.to_string()
}

/// Build a LineageNode from the graph's adjacency data.
fn build_lineage_node(
    hash: &str,
    uris: &HashMap<String, String>,
    reverse: &HashMap<String, Vec<(String, substrate::BondRelation)>>,
) -> LineageNode {
    let uri = uris
        .get(hash)
        .cloned()
        .unwrap_or_else(|| format!("cmn://unknown.dev/{}", hash));

    let refs = if let Some(raw_refs) = reverse.get(hash) {
        raw_refs
            .iter()
            .map(|(parent_uri, relation)| LineageRef {
                relation: relation.to_string(),
                uri: parent_uri.clone(),
            })
            .collect()
    } else {
        Vec::new()
    };

    LineageNode { uri, refs }
}
