//! Lowering of the deliberately small Janus-QL subset used by this benchmark.
//!
//! The public query form follows the current Janus-QL window model directly:
//! live and historical WINDOW blocks participate in one SPARQL/RSP-QL query.
//! Deprecated DEFINE BASELINE / USING BASELINE syntax is intentionally not
//! accepted or generated here.
use janus::parsing::janusql_parser::{
    JanusQLParser, ParsedJanusQuery, UnionBranch, WindowDefinition,
};
use std::{
    fs,
    path::Path,
    time::{Duration, Instant},
};

pub type TimestampBounds = (u64, u64);

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

#[derive(Debug, Clone)]
pub struct LogicalPlan {
    pub live_window: WindowDefinition,
    pub historical_window: WindowDefinition,
    pub join_variable: String,
    pub value_predicate: String,
    pub historical_aggregate: HistoricalAggregate,
    pub condition: GreaterThanMultiplier,
}

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

    pub fn lower(parsed: &ParsedJanusQuery) -> Result<Self, String> {
        Ok(Self::lower_timed(parsed)?.0)
    }

    pub fn lower_timed(parsed: &ParsedJanusQuery) -> Result<(Self, Duration, Duration), String> {
        let lowering_start = Instant::now();
        reject_deprecated_baseline_syntax(parsed)?;
        if parsed.ast.union_branches.is_empty() {
            return Err(
                "Federated-Janus requires top-level UNION branches; adjacent WINDOW blocks are conjunctive"
                    .into(),
            );
        }
        validate_top_level_aggregate_shape(parsed)?;
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
    const MILLISECONDS_PER_SECOND: u64 = 1_000;
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

    pub fn live_bounds(&self, evaluation_time: u64) -> Result<(u64, u64), String> {
        Ok((
            evaluation_time
                .checked_sub(self.live_window.width)
                .ok_or_else(|| "evaluation time precedes live RANGE".to_string())?,
            evaluation_time,
        ))
    }

    /// Resolve both Janus-QL windows for a millisecond clock.  Janus-QL window
    /// literals are seconds, whereas continuously published RDF events carry
    /// millisecond timestamps.
    pub fn continuous_bounds_ms(
        &self,
        evaluation_time_ms: u64,
    ) -> Result<(TimestampBounds, TimestampBounds), String> {
        let historical_offset_ms = self
            .historical_window
            .offset
            .ok_or_else(|| "historical window has no OFFSET".to_string())?
            .checked_mul(Self::MILLISECONDS_PER_SECOND)
            .ok_or_else(|| "historical OFFSET overflows milliseconds".to_string())?;
        let historical_range_ms = self
            .historical_window
            .width
            .checked_mul(Self::MILLISECONDS_PER_SECOND)
            .ok_or_else(|| "historical RANGE overflows milliseconds".to_string())?;
        let historical_start = evaluation_time_ms
            .checked_sub(historical_offset_ms)
            .ok_or_else(|| "evaluation time precedes historical OFFSET".to_string())?;
        let historical_end = historical_start
            .checked_add(historical_range_ms)
            .ok_or_else(|| "historical RANGE overflows milliseconds".to_string())?;
        let live_range_ms = self
            .live_window
            .width
            .checked_mul(Self::MILLISECONDS_PER_SECOND)
            .ok_or_else(|| "live RANGE overflows milliseconds".to_string())?;
        let live_start = evaluation_time_ms
            .checked_sub(live_range_ms)
            .ok_or_else(|| "evaluation time precedes live RANGE".to_string())?;
        Ok((
            (historical_start, historical_end),
            (live_start, evaluation_time_ms),
        ))
    }

    pub fn lower(parsed: &ParsedJanusQuery) -> Result<Self, String> {
        reject_deprecated_baseline_syntax(parsed)?;
        if parsed.live_windows.len() != 1 || parsed.historical_windows.len() != 1 {
            return Err(
                "Federated-Janus currently requires exactly one live and one historical Janus window"
                    .into(),
            );
        }
        validate_top_level_aggregate_shape(parsed)?;

        let live_window = parsed.live_windows[0].clone();
        let historical_window = parsed.historical_windows[0].clone();
        let live_clause = find_window_body(parsed, &live_window)
            .ok_or_else(|| "live WINDOW body is missing".to_string())?;
        let historical_clause = find_window_body(parsed, &historical_window)
            .ok_or_else(|| "historical WINDOW body is missing".to_string())?;

        lower_pair(
            parsed,
            live_window,
            historical_window,
            &live_clause,
            &historical_clause,
        )
    }

    fn lower_union_branch(parsed: &ParsedJanusQuery, branch: &UnionBranch) -> Result<Self, String> {
        if branch.where_windows.len() != 2 {
            return Err(
                "each federated UNION branch requires exactly one live WINDOW and one historical WINDOW"
                    .into(),
            );
        }

        let mut live: Option<(WindowDefinition, String)> = None;
        let mut historical: Option<(WindowDefinition, String)> = None;

        for clause in &branch.where_windows {
            if let Some(window) = parsed.live_windows.iter().find(|window| {
                same_identifier(&clause.identifier, &window.window_name, &parsed.prefixes)
            }) {
                if live.is_some() {
                    return Err("each UNION branch may contain only one live WINDOW".into());
                }
                live = Some((window.clone(), clause.body.clone()));
                continue;
            }
            if let Some(window) = parsed.historical_windows.iter().find(|window| {
                same_identifier(&clause.identifier, &window.window_name, &parsed.prefixes)
            }) {
                if historical.is_some() {
                    return Err("each UNION branch may contain only one historical WINDOW".into());
                }
                historical = Some((window.clone(), clause.body.clone()));
                continue;
            }
            return Err(format!(
                "UNION branch references undeclared or unsupported WINDOW '{}'",
                clause.identifier
            ));
        }

        let (live_window, live_body) =
            live.ok_or_else(|| "UNION branch is missing its live WINDOW".to_string())?;
        let (historical_window, historical_body) = historical
            .ok_or_else(|| "UNION branch is missing its historical WINDOW".to_string())?;

        lower_pair(
            parsed,
            live_window,
            historical_window,
            &live_body,
            &historical_body,
        )
    }
}

pub fn sensor_pair_query(sensor_id: u32) -> String {
    format!(
        "PREFIX ex: <https://example.org/>\n\nREGISTER RStream ex:anomalies{sensor_id} AS\nSELECT ?sensor ?current (AVG(?historical) AS ?historicalAverage)\nFROM NAMED WINDOW ex:live{sensor_id} ON STREAM <https://example.org/sensors/{sensor_id}/live> [RANGE 60 STEP 30]\nFROM NAMED WINDOW ex:history{sensor_id} ON LOG <https://example.org/sensors/{sensor_id}/history> [OFFSET 2592060 RANGE 2592000 STEP 30]\nWHERE {{\n  WINDOW ex:live{sensor_id} {{\n    ?sensor ex:value ?current .\n  }}\n  WINDOW ex:history{sensor_id} {{\n    ?sensor ex:value ?historical .\n  }}\n}}\nGROUP BY ?sensor ?current\nHAVING (?current > 1.3 * AVG(?historical))\n"
    )
}

pub fn generate_federated_anomaly_query(source_count: usize) -> String {
    assert!(
        source_count > 0,
        "a federated query needs at least one source pair"
    );
    let mut query = String::from(
        "PREFIX ex: <https://example.org/>\n\nREGISTER RStream ex:anomalies AS\nSELECT ?sensor ?current (AVG(?historical) AS ?historicalAverage)\n",
    );
    for id in 1..=source_count {
        query.push_str(&format!(
            "FROM NAMED WINDOW ex:live{id} ON STREAM <https://example.org/sensors/{id}/live> [RANGE 60 STEP 30]\nFROM NAMED WINDOW ex:history{id} ON LOG <https://example.org/sensors/{id}/history> [OFFSET 2592060 RANGE 2592000 STEP 30]\n"
        ));
    }
    query.push_str("WHERE {\n");
    for id in 1..=source_count {
        if id > 1 {
            query.push_str("  UNION\n");
        }
        query.push_str(&format!(
            "  {{\n    WINDOW ex:live{id} {{\n      ?sensor ex:value ?current .\n    }}\n    WINDOW ex:history{id} {{\n      ?sensor ex:value ?historical .\n    }}\n  }}\n"
        ));
    }
    query.push_str("}\nGROUP BY ?sensor ?current\nHAVING (?current > 1.3 * AVG(?historical))\n");
    query
}

fn reject_deprecated_baseline_syntax(parsed: &ParsedJanusQuery) -> Result<(), String> {
    if !parsed.ast.baseline_definitions.is_empty() || !parsed.ast.baseline_uses.is_empty() {
        return Err(
            "DEFINE BASELINE / USING BASELINE are deprecated and are not accepted by Federated-Janus"
                .into(),
        );
    }
    Ok(())
}

fn validate_top_level_aggregate_shape(parsed: &ParsedJanusQuery) -> Result<(), String> {
    if parsed.ast.group_by_clause.is_none() {
        return Err("historical AVG query requires a top-level GROUP BY clause".into());
    }
    if parsed.ast.having_clause.is_none() {
        return Err("anomaly query requires a top-level HAVING comparison".into());
    }
    parse_avg(&parsed.ast.select_clause)?;
    Ok(())
}

fn find_window_body(parsed: &ParsedJanusQuery, window: &WindowDefinition) -> Option<String> {
    parsed
        .ast
        .where_windows
        .iter()
        .find(|clause| same_identifier(&clause.identifier, &window.window_name, &parsed.prefixes))
        .map(|clause| clause.body.clone())
}

fn lower_pair(
    parsed: &ParsedJanusQuery,
    live_window: WindowDefinition,
    historical_window: WindowDefinition,
    live_body: &str,
    historical_body: &str,
) -> Result<LogicalPlan, String> {
    let (live_subject, predicate, current_variable) = parse_triple(live_body)?;
    let (historical_subject, historical_predicate, historical_input) =
        parse_triple(historical_body)?;
    if live_subject != historical_subject || predicate != historical_predicate {
        return Err(
            "live and historical WINDOW blocks must join on the same subject and predicate".into(),
        );
    }

    let (function, input_variable, output_variable) = parse_avg(&parsed.ast.select_clause)?;
    if input_variable != historical_input {
        return Err("top-level AVG must aggregate the historical WINDOW value variable".into());
    }

    let group_vars = parse_group_by_variables(
        parsed
            .ast
            .group_by_clause
            .as_deref()
            .ok_or_else(|| "historical AVG requires GROUP BY".to_string())?,
    )?;
    if !group_vars.contains(&live_subject) || !group_vars.contains(&current_variable) {
        return Err(
            "GROUP BY must include the shared sensor variable and the current live value".into(),
        );
    }

    let (having_current, multiplier, having_historical) = parse_having_condition(
        parsed
            .ast
            .having_clause
            .as_deref()
            .ok_or_else(|| "anomaly query requires HAVING".to_string())?,
    )?;
    if having_current != current_variable || having_historical != input_variable {
        return Err(
            "HAVING must compare the live value with the AVG of the historical value variable"
                .into(),
        );
    }

    Ok(LogicalPlan {
        live_window,
        historical_window,
        join_variable: live_subject.clone(),
        value_predicate: expand(&predicate, &parsed.prefixes),
        historical_aggregate: HistoricalAggregate {
            function,
            input_variable,
            output_variable: output_variable.clone(),
            group_variable: live_subject,
        },
        condition: GreaterThanMultiplier {
            current_variable,
            average_variable: output_variable,
            multiplier,
        },
    })
}

fn parse_avg(select: &str) -> Result<(String, String, String), String> {
    let start = select
        .find("AVG(")
        .ok_or_else(|| "SELECT must contain AVG(?historical) AS ?variable".to_string())?
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

fn parse_group_by_variables(group_by: &str) -> Result<Vec<String>, String> {
    let rest = group_by
        .trim()
        .strip_prefix("GROUP BY")
        .ok_or_else(|| "GROUP BY clause must start with GROUP BY".to_string())?;
    let vars = rest
        .split_whitespace()
        .filter(|v| v.starts_with('?'))
        .map(str::to_string)
        .collect::<Vec<_>>();
    if vars.is_empty() {
        return Err("GROUP BY must contain variables".into());
    }
    Ok(vars)
}

fn parse_having_condition(having: &str) -> Result<(String, f64, String), String> {
    let mut expr = having
        .trim()
        .strip_prefix("HAVING")
        .ok_or_else(|| "HAVING clause must start with HAVING".to_string())?
        .trim();
    if expr.starts_with('(') && expr.ends_with(')') {
        expr = expr[1..expr.len() - 1].trim();
    }
    let (current, rhs) = expr
        .split_once('>')
        .ok_or_else(|| "HAVING must use >".to_string())?;
    let current = current.trim().to_string();
    if !current.starts_with('?') {
        return Err("HAVING left-hand side must be a variable".into());
    }

    let (multiplier, aggregate) = if let Some((factor, aggregate)) = rhs.split_once('*') {
        (
            factor
                .trim()
                .parse::<f64>()
                .map_err(|_| "HAVING multiplier must be numeric".to_string())?,
            aggregate.trim(),
        )
    } else {
        (1.0, rhs.trim())
    };
    let avg_start = aggregate
        .find("AVG(")
        .ok_or_else(|| "HAVING must compare against AVG(?historical)".to_string())?
        + 4;
    let avg_end = aggregate[avg_start..]
        .find(')')
        .ok_or_else(|| "unterminated AVG in HAVING".to_string())?
        + avg_start;
    let historical = aggregate[avg_start..avg_end].trim().to_string();
    if !historical.starts_with('?') {
        return Err("HAVING AVG input must be a variable".into());
    }
    Ok((current, multiplier, historical))
}

fn parse_triple(body: &str) -> Result<(String, String, String), String> {
    let line = body
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with('{'))
        .ok_or_else(|| "expected a triple pattern".to_string())?
        .trim_end_matches('.')
        .trim();
    let parts: Vec<_> = line.split_whitespace().collect();
    if parts.len() != 3 || !parts[0].starts_with('?') || !parts[2].starts_with('?') {
        return Err("benchmark subset requires ?subject predicate ?object triple patterns".into());
    }
    Ok((parts[0].into(), parts[1].into(), parts[2].into()))
}

fn expand(term: &str, prefixes: &std::collections::HashMap<String, String>) -> String {
    term.split_once(':')
        .and_then(|(prefix, local)| prefixes.get(prefix).map(|base| format!("{base}{local}")))
        .unwrap_or_else(|| term.trim_matches(&['<', '>'][..]).into())
}

fn same_identifier(
    identifier: &str,
    name: &str,
    prefixes: &std::collections::HashMap<String, String>,
) -> bool {
    expand(identifier, prefixes) == name
}
