use anyhow::Result;
use once_cell::sync::Lazy;
use regex::Regex;

#[derive(Debug)]
pub struct SelectStmt {
    pub columns: Vec<String>,
    pub table: String,
    pub conditions: Vec<Condition>,
}

#[derive(Debug)]
pub struct Condition {
    pub column: String,
    pub op: String,
    pub value: String,
}

static SELECT_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?is)^\s*select\s+(?P<cols>.+?)\s+from\s+(?P<table>\w+)(?:\s+where\s+(?P<where>.+?))?\s*;?\s*$")
        .unwrap()
});

static COND_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?is)^\s*(?P<col>\w+)\s*(?P<op>=|!=|<=|>=|<|>)\s*(?P<val>'[^']*'|"[^"]*"|\d+|\w+)\s*$"#,
    )
    .unwrap()
});

pub fn parse_select(sql: &str) -> Result<SelectStmt, String> {
    let caps = SELECT_RE
        .captures(sql)
        .ok_or_else(|| "Invalid SELECT statement".to_string())?;

    let cols_raw = caps.name("cols").unwrap().as_str();
    let table = caps.name("table").unwrap().as_str().to_string();

    let columns = cols_raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();

    let mut conditions = Vec::new();

    if let Some(where_part) = caps.name("where") {
        let where_raw = where_part.as_str();

        for cond_str in where_raw.split(|c| c == 'A' || c == 'a') {
            // NOTE: this is NOT correct splitting logic for general SQL
            // We'll do a more controlled AND split below instead.
        }

        for cond_str in where_raw.split(|_| false) {
            let _ = cond_str;
        }

        // Proper simple AND split:
        let and_re = Regex::new(r"(?i)\s+and\s+").unwrap();

        let parts = and_re
            .split(where_raw)
            .map(str::trim)
            .filter(|s| !s.is_empty());

        for cond_str in parts {
            let c = COND_RE
                .captures(cond_str)
                .ok_or_else(|| format!("Invalid condition: {cond_str}"))?;

            let mut val = c.name("val").unwrap().as_str().to_string();

            // remove quotes if string literal
            if (val.starts_with('\'') && val.ends_with('\''))
                || (val.starts_with('"') && val.ends_with('"'))
            {
                val = val[1..val.len() - 1].to_string();
            }

            conditions.push(Condition {
                column: c.name("col").unwrap().as_str().to_string(),
                op: c.name("op").unwrap().as_str().to_string(),
                value: val,
            });
        }
    }

    Ok(SelectStmt {
        columns,
        table,
        conditions,
    })
}

#[derive(Debug)]
pub struct CreateTableStmt {
    pub table: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug)]
pub struct ColumnDef {
    pub name: String,
    pub ty: Option<String>,
}

static CREATE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?is)^\s*create\s+table\s+(?P<table>\w+)\s*\(\s*(?P<body>.*?)\s*\)\s*;?\s*$")
        .unwrap()
});

static COL_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?is)^\s*(?P<name>\w+)(?:\s+(?P<ty>\w+))?").unwrap());

pub fn parse_create(sql: &str) -> Result<CreateTableStmt, String> {
    let caps = CREATE_RE
        .captures(sql)
        .ok_or_else(|| "Invalid CREATE TABLE statement".to_string())?;

    let table = caps.name("table").unwrap().as_str().to_string();
    let body = caps.name("body").unwrap().as_str();

    let mut columns = Vec::new();

    for chunk in body.split(',') {
        let chunk = chunk.trim();
        if chunk.is_empty() {
            continue;
        }

        let c = COL_RE
            .captures(chunk)
            .ok_or_else(|| format!("Invalid column definition: {chunk}"))?;

        let name = c.name("name").unwrap().as_str().to_string();
        let ty = c.name("ty").map(|m| m.as_str().to_string());

        columns.push(ColumnDef { name, ty });
    }

    Ok(CreateTableStmt { table, columns })
}
