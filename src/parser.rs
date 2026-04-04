use anyhow::Result;
use once_cell::sync::Lazy;
use regex::Regex;

#[derive(Debug)]
pub struct SelectStmt {
    pub columns: Vec<String>,
    pub table: String,
    pub conditions: Vec<Condition>,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Default, Clone)]
pub struct CreateTableStmt {
    pub table: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub ty: Option<String>,
}

// static CREATE_RE: Lazy<Regex> = Lazy::new(|| {
//     Regex::new(
//         r"(?is)^\s*create\s+table\s+(?P<table>\w+)\s*\(\s*(?P<body>.*?)\s*\)\s*;?\s*$")
//         .unwrap()
// });

// static CREATE_RE: Lazy<Regex> = Lazy::new(|| {
//     Regex::new(
//         r#"(?is)^\s*create\s+table\s+(?P<table>"[^"]+"|\w+)\s*\(\s*(?P<body>.*?)\s*\)\s*;?\s*$"#,
//     )
//     .unwrap()
// });

static CREATE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?is)^\s*create\s+table\s+(?P<table>"[^"]+"|\w+)\s*\(\s*(?P<body>.*?)\s*\)\s*;?\s*$"#,
    )
    .unwrap()
});

// static COL_RE: Lazy<Regex> =
//     Lazy::new(|| Regex::new(r"(?is)^\s*(?P<name>\w+)(?:\s+(?P<ty>\w+))?").unwrap());

static COL_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"^\s*(?P<name>"[^"]+"|\w+)(?:\s+(?P<ty>\w+))?"#).unwrap());

fn unquote_ident(s: &str) -> String {
    if s.starts_with('"') && s.ends_with('"') {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

pub fn parse_create(sql: &str) -> Result<CreateTableStmt, String> {
    let caps = CREATE_RE
        .captures(sql)
        .ok_or("Invalid CREATE TABLE statement")?;

    let table = unquote_ident(caps.name("table").unwrap().as_str());
    let body = caps.name("body").unwrap().as_str();

    let mut columns = Vec::new();

    for part in body.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        let caps = COL_RE
            .captures(part)
            .ok_or_else(|| format!("Invalid column definition: {}", part))?;

        let name = unquote_ident(caps.name("name").unwrap().as_str());
        let ty = caps.name("ty").map(|m| m.as_str().to_string());

        columns.push(ColumnDef { name, ty });
    }

    Ok(CreateTableStmt { table, columns })
}

#[derive(Debug, PartialEq, Clone, Default)]
pub struct CreateIndexStmt {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
}

static CREATE_INDEX_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?is)^\s*create\s+(?:unique\s+)?index\s+(?:if\s+not\s+exists\s+)?(?P<name>"[^"]+"|\w+)\s+on\s+(?P<table>"[^"]+"|\w+)\s*\(\s*(?P<cols>.*?)\s*\)\s*;?\s*$"#
    )
    .unwrap()
});

pub fn parse_create_index(sql: &str) -> Result<CreateIndexStmt, String> {
    let caps = CREATE_INDEX_RE
        .captures(sql)
        .ok_or("Invalid CREATE INDEX statement")?;

    let name = unquote_ident(caps.name("name").unwrap().as_str());
    let table = unquote_ident(caps.name("table").unwrap().as_str());

    let cols_raw = caps.name("cols").unwrap().as_str();

    let columns = cols_raw
        .split(',')
        .map(|c| unquote_ident(c.trim()))
        .collect::<Vec<_>>();

    Ok(CreateIndexStmt {
        name,
        table,
        columns,
    })
}

#[test]
fn test_parse_create_index() {
    let r = parse_create_index("CREATE INDEX idx_companies_country on companies (country)");
    assert!(r.is_ok());
    let r = r.unwrap();
    let e = CreateIndexStmt {
        name: "idx_companies_country".to_string(),
        table: "companies".to_string(),
        columns: ["country".to_string()].to_vec(),
    };
    assert_eq!(r, e)
}
