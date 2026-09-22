//! Lowering for the deliberately narrow three-input planning experiment.
//!
//! Static metadata is ordinary SPARQL in a named graph. Historical aggregation
//! is expressed directly with WINDOW + AVG + GROUP BY + HAVING; deprecated
//! DEFINE BASELINE / USING BASELINE syntax is not generated.
use janus::parsing::janusql_parser::JanusQLParser;

#[derive(Debug, Clone)]
pub struct PlanningLogicalQuery {
    pub text: String,
    pub source_pairs: usize,
    pub metadata_graph: String,
}

impl PlanningLogicalQuery {
    pub fn from_text(text: &str, source_pairs: usize) -> Result<Self, String> {
        if text.contains("DEFINE BASELINE") || text.contains("USING BASELINE") {
            return Err("deprecated baseline syntax is not accepted in planning queries".into());
        }
        let parsed = JanusQLParser::new()
            .map_err(|e| e.to_string())?
            .parse(text)
            .map_err(|e| e.to_string())?;
        let graph = "GRAPH <https://example.org/metadata>";
        if parsed.live_windows.len() != source_pairs
            || parsed.historical_windows.len() != source_pairs
            || !parsed.ast.where_clause.contains(graph)
            || !parsed.ast.where_clause.contains("ex:locatedIn ex:RoomA")
            || !parsed.ast.select_clause.contains("AVG(?historical)")
            || parsed.ast.group_by_clause.is_none()
            || parsed.ast.having_clause.is_none()
        {
            return Err(
                "planning query must contain all live/history windows, the metadata named graph, and the top-level aggregate/HAVING shape"
                    .into(),
            );
        }
        Ok(Self {
            text: text.into(),
            source_pairs,
            metadata_graph: "https://example.org/metadata".into(),
        })
    }
}

pub fn generate_planning_query(source_pairs: usize) -> String {
    assert!(source_pairs > 0, "planning query needs at least one source pair");
    let mut q = String::from(
        "PREFIX ex: <https://example.org/>\n\nREGISTER RStream ex:anomalies AS\nSELECT ?sensor ?current (AVG(?historical) AS ?historicalAverage)\n",
    );
    for id in 1..=source_pairs {
        q.push_str(&format!(
            "FROM NAMED WINDOW ex:live{id} ON STREAM <https://example.org/sensors/{id}/live> [RANGE 60 STEP 30]\nFROM NAMED WINDOW ex:history{id} ON LOG <https://example.org/sensors/{id}/history> [OFFSET 2592060 RANGE 2592000 STEP 30]\n"
        ));
    }
    q.push_str("WHERE {\n");
    for id in 1..=source_pairs {
        if id > 1 {
            q.push_str("  UNION\n");
        }
        q.push_str(&format!(
            "  {{\n    WINDOW ex:live{id} {{\n      ?sensor ex:value ?current .\n    }}\n    WINDOW ex:history{id} {{\n      ?sensor ex:value ?historical .\n    }}\n    GRAPH <https://example.org/metadata> {{\n      ?sensor ex:locatedIn ex:RoomA .\n    }}\n  }}\n"
        ));
    }
    q.push_str(
        "}\nGROUP BY ?sensor ?current\nHAVING (?current > 1.3 * AVG(?historical))\n",
    );
    q
}
