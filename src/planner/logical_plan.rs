//! Lowering of the deliberately small Janus-QL subset used by this benchmark.
use janus::parsing::janusql_parser::{
    JanusQLParser, ParsedJanusQuery, UnionBranch, WindowDefinition,
};
use std::{
    fs,
    path::Path,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, PartialEq)]
pub struct HistoricalAggregate {
    pub function: String,
    pub input_variable: String,
    pub output_variable: String,
    pub group_variable: String,
}
#[derive(Debug, Clone, PartialEq)]
pub struct GreaterThanMultiplier {
    pub current_variable: String,
    pub average_variable: String,
    pub multiplier: f64,
}

/// The query shape understood by the three manually selected federated plans.
/// It is intentionally not a general Janus-QL federation representation.
#[derive(Debug, Clone)]
pub struct LogicalPlan {
    pub live_window: WindowDefinition,
    pub historical_window: WindowDefinition,
    pub join_variable: String,
    pub value_predicate: String,
    pub historical_aggregate: HistoricalAggregate,
    pub condition: GreaterThanMultiplier,
}

/// One logical Janus-QL query decomposed into its explicit, independent UNION
/// branches.  This is deliberately not a plan selector: each branch is a
/// source-specific fragment already expressed by the query author.
#[derive(Debug, Clone)]
pub struct FederatedLogicalPlan {
    pub branches: Vec<LogicalPlan>,
}

impl FederatedLogicalPlan {
    pub fn from_text(text: &str) -> Result<Self, String> {
        let parsed = JanusQLParser::new()
            .map_err(|e| e.to_string())?
            .parse(text)
            .map_err(|e| e.to_string())?;
        Self::lower(&parsed)
    }

    /// Lower the UNION structure already represented by Janus.  No source
    /// pairing or branch inference is performed here.
    pub fn lower(parsed: &ParsedJanusQuery) -> Result<Self, String> {
        Ok(Self::lower_timed(parsed)?.0)
    }

    /// Separates coordinator-level lowering validation from construction of the
    /// source-specific branch fragments for benchmark setup accounting.
    pub fn lower_timed(parsed: &ParsedJanusQuery) -> Result<(Self, Duration, Duration), String> {
        let lowering_start = Instant::now();
        if parsed.ast.union_branches.is_empty() {
            return Err("Federated-Janus requires top-level UNION branches; adjacent WINDOW blocks are conjunctive".into());
        }
        let lowering = lowering_start.elapsed();
        let decomposition_start = Instant::now();
        let branches = parsed
            .ast
            .union_branches
            .iter()
            .map(|branch| LogicalPlan::lower_union_branch(parsed, branch))
            .collect::<Result<Vec<_>, _>>()?;
        Ok((Self { branches }, lowering, decomposition_start.elapsed()))
    }
}

impl LogicalPlan {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, String> {
        Self::from_text(&fs::read_to_string(path.as_ref()).map_err(|e| e.to_string())?)
    }
    pub fn from_text(text: &str) -> Result<Self, String> {
        Self::lower(
            &JanusQLParser::new()
                .map_err(|e| e.to_string())?
                .parse(text)
                .map_err(|e| e.to_string())?,
        )
    }
    pub fn historical_bounds(&self, evaluation_time: u64) -> Result<(u64, u64), String> {
        self.historical_window
            .resolve_historical_bounds(evaluation_time)
            .ok_or_else(|| {
                "Janus could not resolve historical bounds for this evaluation time".into()
            })
    }
    /// The lightweight deterministic live evaluator uses [T - RANGE, T).
    pub fn live_bounds(&self, evaluation_time: u64) -> Result<(u64, u64), String> {
        Ok((
            evaluation_time
                .checked_sub(self.live_window.width)
                .ok_or_else(|| "evaluation time precedes live RANGE".to_string())?,
            evaluation_time,
        ))
    }
    pub fn lower(parsed: &ParsedJanusQuery) -> Result<Self, String> {
        if parsed.live_windows.len() != 1 || parsed.historical_windows.len() != 1 {
            return Err("Federated-Janus currently requires exactly one live and one historical Janus window".into());
        }
        let live_window = parsed.live_windows[0].clone();
        let historical_window = parsed.historical_windows[0].clone();
        let baseline = parsed.ast.baseline_definitions.first().ok_or_else(|| {
            "the benchmark subset requires DEFINE BASELINE for the historical aggregate".to_string()
        })?;
        if baseline.source_window != historical_window.window_name {
            return Err(
                "historical baseline must be defined over the declared historical window".into(),
            );
        }
        let (function, input_variable, output_variable) = parse_avg(&baseline.select_clause)?;
        let group_variable = baseline
            .group_by_clause
            .as_deref()
            .and_then(|g| g.strip_prefix("GROUP BY"))
            .map(str::trim)
            .filter(|g| !g.contains(char::is_whitespace))
            .ok_or_else(|| "historical AVG requires one GROUP BY variable".to_string())?
            .to_string();
        let (live_subject, predicate, current_variable) = parsed
            .ast
            .where_windows
            .iter()
            .find(|w| same_identifier(&w.identifier, &live_window.window_name, &parsed.prefixes))
            .ok_or_else(|| "live WINDOW body is missing".to_string())
            .and_then(|w| parse_triple(&w.body))?;
        if live_subject != group_variable {
            return Err(
                "live triple subject and historical GROUP BY must be the same join variable".into(),
            );
        }
        let (historical_subject, historical_predicate, historical_input) =
            parse_triple(&baseline.where_clause)?;
        if historical_subject != group_variable
            || historical_predicate != predicate
            || historical_input != input_variable
        {
            return Err(
                "historical baseline must AVG the same predicate grouped by the live join variable"
                    .into(),
            );
        }
        let condition = parse_condition(&parsed.ast.where_clause)?;
        if condition.current_variable != current_variable
            || condition.average_variable != output_variable
        {
            return Err("FILTER must compare the live value with the named historical AVG".into());
        }
        Ok(Self {
            live_window,
            historical_window,
            join_variable: group_variable.clone(),
            value_predicate: expand(&predicate, &parsed.prefixes),
            historical_aggregate: HistoricalAggregate {
                function,
                input_variable,
                output_variable,
                group_variable,
            },
            condition,
        })
    }

    fn lower_union_branch(parsed: &ParsedJanusQuery, branch: &UnionBranch) -> Result<Self, String> {
        if branch.where_windows.len() != 1 {
            return Err(
                "each federated UNION branch requires exactly one live WINDOW block".into(),
            );
        }
        let where_window = &branch.where_windows[0];
        let live_window = parsed
            .live_windows
            .iter()
            .find(|window| {
                same_identifier(
                    &where_window.identifier,
                    &window.window_name,
                    &parsed.prefixes,
                )
            })
            .cloned()
            .ok_or_else(|| {
                "each federated UNION branch WINDOW must refer to a declared live source"
                    .to_string()
            })?;
        let baseline_name = graph_identifier(&branch.body).ok_or_else(|| {
            "each federated UNION branch requires one historical baseline GRAPH".to_string()
        })?;
        let baseline = parsed
            .ast
            .baseline_definitions
            .iter()
            .find(|definition| same_identifier(&baseline_name, &definition.name, &parsed.prefixes))
            .ok_or_else(|| "UNION branch GRAPH must name a DEFINE BASELINE".to_string())?;
        let historical_window = parsed
            .historical_windows
            .iter()
            .find(|window| window.window_name == baseline.source_window)
            .cloned()
            .ok_or_else(|| {
                "baseline must be defined over a declared historical window".to_string()
            })?;
        let (function, input_variable, output_variable) = parse_avg(&baseline.select_clause)?;
        let group_variable = baseline
            .group_by_clause
            .as_deref()
            .and_then(|g| g.strip_prefix("GROUP BY"))
            .map(str::trim)
            .filter(|g| !g.contains(char::is_whitespace))
            .ok_or_else(|| "historical AVG requires one GROUP BY variable".to_string())?
            .to_string();
        let (live_subject, predicate, current_variable) = parse_triple(&where_window.body)?;
        let (historical_subject, historical_predicate, historical_input) =
            parse_triple(&baseline.where_clause)?;
        if live_subject != group_variable
            || historical_subject != group_variable
            || historical_predicate != predicate
            || historical_input != input_variable
        {
            return Err(
                "UNION branch must join one live value to the matching historical AVG".into(),
            );
        }
        let condition = parse_condition(&branch.body)?;
        if condition.current_variable != current_variable
            || condition.average_variable != output_variable
        {
            return Err(
                "UNION branch FILTER must compare its live value with its baseline AVG".into(),
            );
        }
        Ok(Self {
            live_window,
            historical_window,
            join_variable: group_variable.clone(),
            value_predicate: expand(&predicate, &parsed.prefixes),
            historical_aggregate: HistoricalAggregate {
                function,
                input_variable,
                output_variable,
                group_variable,
            },
            condition,
        })
    }
}

/// Deterministic Janus-QL text for one independently addressable sensor pair.
/// The source-oriented benchmark parses and lowers this text for every pair;
/// it never constructs logical plans directly.
pub fn sensor_pair_query(sensor_id: u32) -> String {
    format!("PREFIX ex: <https://example.org/>\n\nFROM NAMED WINDOW ex:live{sensor_id} ON STREAM <https://example.org/sensors/{sensor_id}/live> [RANGE 60 STEP 30]\nFROM NAMED WINDOW ex:history{sensor_id} ON LOG <https://example.org/sensors/{sensor_id}/history> [OFFSET 2592000 RANGE 2592000 STEP 30]\n\nDEFINE BASELINE ex:historicalAverage{sensor_id} ON WINDOW ex:history{sensor_id} AS\nSELECT ?sensor (AVG(?historical) AS ?historicalAverage)\nWHERE {{\n  ?sensor ex:value ?historical .\n}}\nGROUP BY ?sensor\n\nREGISTER RStream ex:anomalies{sensor_id} AS\nUSING BASELINE ex:historicalAverage{sensor_id}\nSELECT ?sensor ?current ?historicalAverage\nWHERE {{\n  WINDOW ex:live{sensor_id} {{\n    ?sensor ex:value ?current .\n  }}\n  GRAPH ex:historicalAverage{sensor_id} {{\n    ?sensor ex:historicalAverage ?historicalAverage .\n  }}\n  FILTER(?current > 1.3 * ?historicalAverage)\n}}\n")
}

/// Deterministically generates one Janus-QL query whose standard SPARQL UNION
/// has one independently addressable live/history branch per sensor.
pub fn generate_federated_anomaly_query(source_count: usize) -> String {
    assert!(
        source_count > 0,
        "a federated query needs at least one source pair"
    );
    let mut query = String::from("PREFIX ex: <https://example.org/>\n\n");
    for id in 1..=source_count {
        query.push_str(&format!(
            "FROM NAMED WINDOW ex:live{id} ON STREAM <https://example.org/sensors/{id}/live> [RANGE 60 STEP 30]\nFROM NAMED WINDOW ex:history{id} ON LOG <https://example.org/sensors/{id}/history> [OFFSET 2592000 RANGE 2592000 STEP 30]\n"
        ));
    }
    query.push('\n');
    for id in 1..=source_count {
        query.push_str(&format!(
            "DEFINE BASELINE ex:historicalAverage{id} ON WINDOW ex:history{id} AS\nSELECT ?sensor (AVG(?historical) AS ?historicalAverage)\nWHERE {{\n  ?sensor ex:value ?historical .\n}}\nGROUP BY ?sensor\n\n"
        ));
    }
    query.push_str("REGISTER RStream ex:anomalies AS\n");
    for id in 1..=source_count {
        query.push_str(&format!("USING BASELINE ex:historicalAverage{id}\n"));
    }
    query.push_str("SELECT ?sensor ?current ?historicalAverage\nWHERE {\n");
    for id in 1..=source_count {
        if id > 1 {
            query.push_str("  UNION\n");
        }
        query.push_str(&format!(
            "  {{\n    WINDOW ex:live{id} {{\n      ?sensor ex:value ?current .\n    }}\n    GRAPH ex:historicalAverage{id} {{\n      ?sensor ex:historicalAverage ?historicalAverage .\n    }}\n    FILTER(?current > 1.3 * ?historicalAverage)\n  }}\n"
        ));
    }
    query.push_str("}\n");
    query
}

fn graph_identifier(body: &str) -> Option<String> {
    let start = body.find("GRAPH")? + "GRAPH".len();
    body[start..].split_whitespace().next().map(str::to_string)
}
fn parse_avg(select: &str) -> Result<(String, String, String), String> {
    let start = select
        .find("AVG(")
        .ok_or_else(|| "historical baseline must contain AVG(...) AS ?variable".to_string())?
        + 4;
    let end = select[start..]
        .find(')')
        .ok_or_else(|| "unterminated AVG".to_string())?
        + start;
    let input = select[start..end].trim().to_string();
    let output = select[end + 1..]
        .trim_start()
        .strip_prefix("AS")
        .ok_or_else(|| "AVG must use AS ?variable".to_string())?
        .trim_start()
        .split(|c: char| c == ')' || c.is_whitespace())
        .next()
        .unwrap_or("")
        .to_string();
    if !input.starts_with('?') || !output.starts_with('?') {
        return Err("AVG input and alias must be variables".into());
    }
    Ok(("AVG".into(), input, output))
}
fn parse_triple(body: &str) -> Result<(String, String, String), String> {
    let line = body
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty() && !l.starts_with("WHERE") && !l.starts_with('{'))
        .ok_or_else(|| "expected a triple pattern".to_string())?
        .trim_end_matches('.')
        .trim();
    let p: Vec<_> = line.split_whitespace().collect();
    if p.len() != 3 || !p[0].starts_with('?') || !p[2].starts_with('?') {
        return Err("benchmark subset requires ?subject predicate ?object triple patterns".into());
    }
    Ok((p[0].into(), p[1].into(), p[2].into()))
}
fn parse_condition(where_clause: &str) -> Result<GreaterThanMultiplier, String> {
    let start = where_clause.find("FILTER(").ok_or_else(|| {
        "benchmark subset requires FILTER(?current > multiplier * ?average)".to_string()
    })? + 7;
    let end = where_clause[start..]
        .find(')')
        .ok_or_else(|| "unterminated FILTER".to_string())?
        + start;
    let (current, rhs) = where_clause[start..end]
        .trim()
        .split_once('>')
        .ok_or_else(|| "FILTER must use >".to_string())?;
    let (multiplier, average) = rhs
        .split_once('*')
        .ok_or_else(|| "FILTER must multiply historical average".to_string())?;
    Ok(GreaterThanMultiplier {
        current_variable: current.trim().into(),
        multiplier: multiplier
            .trim()
            .parse()
            .map_err(|_| "FILTER multiplier must be numeric")?,
        average_variable: average.trim().into(),
    })
}
fn expand(term: &str, prefixes: &std::collections::HashMap<String, String>) -> String {
    term.split_once(':')
        .and_then(|(p, local)| prefixes.get(p).map(|base| format!("{base}{local}")))
        .unwrap_or_else(|| term.trim_matches(&['<', '>'][..]).into())
}
fn same_identifier(
    identifier: &str,
    name: &str,
    prefixes: &std::collections::HashMap<String, String>,
) -> bool {
    expand(identifier, prefixes) == name
}
