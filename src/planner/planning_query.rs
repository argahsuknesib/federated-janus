//! Lowering for the deliberately narrow three-input planning experiment.
//!
//! Static metadata is ordinary SPARQL in a named graph.  Janus already parses
//! and preserves it, so this module adds no Janus-QL syntax.
use janus::parsing::janusql_parser::JanusQLParser;

#[derive(Debug, Clone)]
pub struct PlanningLogicalQuery {
    pub text: String,
    pub source_pairs: usize,
    pub metadata_graph: String,
}

impl PlanningLogicalQuery {
    pub fn from_text(text: &str, source_pairs: usize) -> Result<Self, String> {
        let parsed = JanusQLParser::new()
            .map_err(|e| e.to_string())?
            .parse(text)
            .map_err(|e| e.to_string())?;
        let graph = "GRAPH <https://example.org/metadata>";
        if parsed.live_windows.len() != source_pairs
            || parsed.historical_windows.len() != source_pairs
            || !parsed.rspql_query.contains(graph)
            || !parsed.rspql_query.contains("ex:locatedIn ex:RoomA")
        {
            return Err("planning query must contain all live/history windows and the static metadata named graph".into());
        }
        Ok(Self {
            text: text.into(),
            source_pairs,
            metadata_graph: "https://example.org/metadata".into(),
        })
    }
}

/// One Janus-QL query: windows are independently addressable and metadata is a
/// standard static RDF named graph. UNION keeps the per-source branches disjoint.
pub fn generate_planning_query(source_pairs: usize) -> String {
    let mut q = String::from("PREFIX ex: <https://example.org/>\n\n");
    for id in 1..=source_pairs {
        q.push_str(&format!("FROM NAMED WINDOW ex:live{id} ON STREAM <https://example.org/sensors/{id}/live> [RANGE 60 STEP 30]\nFROM NAMED WINDOW ex:history{id} ON LOG <https://example.org/sensors/{id}/history> [OFFSET 2592000 RANGE 2592000 STEP 30]\n"));
    }
    q.push('\n');
    for id in 1..=source_pairs {
        q.push_str(&format!("DEFINE BASELINE ex:avg{id} ON WINDOW ex:history{id} AS\nSELECT ?sensor (AVG(?historical) AS ?historicalAverage)\nWHERE {{ ?sensor ex:value ?historical . }}\nGROUP BY ?sensor\n\n"));
    }
    q.push_str("REGISTER RStream ex:anomalies AS\n");
    for id in 1..=source_pairs {
        q.push_str(&format!("USING BASELINE ex:avg{id}\n"));
    }
    q.push_str("SELECT ?sensor ?current ?historicalAverage\nWHERE {\n");
    for id in 1..=source_pairs {
        if id > 1 {
            q.push_str(" UNION\n");
        }
        q.push_str(&format!(" {{ WINDOW ex:live{id} {{ ?sensor ex:value ?current . }} GRAPH ex:avg{id} {{ ?sensor ex:historicalAverage ?historicalAverage . }} GRAPH <https://example.org/metadata> {{ ?sensor ex:locatedIn ex:RoomA . }} FILTER(?current > 1.3 * ?historicalAverage) }}\n"));
    }
    q.push_str("}\n");
    q
}
