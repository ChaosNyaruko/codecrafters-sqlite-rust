use anyhow::{Context, Result, bail};
use std::collections::HashMap;
use std::fmt::{self, Write};
use std::fs::File;
use std::io::{SeekFrom, prelude::*};
mod parser;

#[derive(Debug)]
struct Tables<'r> {
    dbinfo: DBInfo,
    reader: &'r File,

    // state
    cur_tbl_name: String,
    cur_rootpage: usize,
    cur_create: parser::CreateTableStmt,

    display: String,
    pos: HashMap<String, usize>, // key: tbl_name, value: rootpage
    content: HashMap<String, parser::CreateTableStmt>, // key: tbl_name, value: Table with column names
}

trait OnColumn {
    fn on_col(&mut self, row: usize, col: usize, v: &ColType);
    fn on_row(&mut self);
    fn finalize(&mut self);
    fn set_type(&mut self, t: u8);
}

impl<'r> OnColumn for Tables<'r> {
    fn on_col(&mut self, row: usize, col: usize, v: &ColType) {
        // schema: type name tbl_name rootpage sql
        if col == 2 {
            if let ColType::Text(text) = v {
                write!(self.display, "{}", text).unwrap();
                self.cur_tbl_name = text.clone();
            }
            if row != self.dbinfo.table_count as usize - 1 {
                write!(self.display, " ").unwrap();
            }
        }
        if col == 3 {
            if let ColType::Integer(o) = v {
                self.cur_rootpage = *o as usize;
            }
        }
        if col == 4 {
            if let ColType::Text(sql) = v {
                // eprintln!("sql:{}", sql);
                let cols = parser::parse_create(&sql).expect(&format!("parse create err: {sql}"));
                // eprintln!("create: {cols:?}");
                self.cur_create = cols;
            }
        }
    }

    fn on_row(&mut self) {
        self.pos
            .insert(self.cur_tbl_name.clone(), self.cur_rootpage);
        assert!(
            self.cur_create.table.contains(&self.cur_tbl_name),
            "create table name should be consistent with the tbl_name field, {} vs {}",
            self.cur_create.table,
            self.cur_tbl_name,
        );
        self.content
            .insert(self.cur_tbl_name.clone(), self.cur_create.clone());
    }

    fn finalize(&mut self) {}

    fn set_type(&mut self, _t: u8) {}
}

fn parse_cell_as_rows(p: &Page, state: &mut dyn OnColumn, reader: &File, db: DBInfo) {
    let page = &p.page;
    state.set_type(p.page_type);
    let cell_offsets = &p.cell_offsets;
    for (ic, offset) in cell_offsets.into_iter().enumerate() {
        let mut buf = &page[*offset as usize..];
        let mut i = 0;
        if p.page_type == 0x0d {
            let (size, j1) = decode_varint(buf);
            i += j1;
            let (_rowid, j2) = decode_varint(&buf[i..]);
            i += j2;

            let U = db.page_size as usize;
            let X = U - 35;
            let M = ((U - 12) * 32 / 255) - 23;
            let P = size as usize;
            let K = M + ((P - M) % (U - 4));
            let mut onpage;
            if P <= X {
                // no overflow
            } else if K <= X {
                // the first K bytes of P are stored on the btree page and the remaining P-K bytes are stored on overflow pages.
                onpage = buf[i..i + K].to_vec();
                let mut next = u32::from_be_bytes(buf[i + K..i + K + 4].try_into().unwrap());
                while next != 0 {
                    let op = parse_page(next as usize - 1, reader, &db, true).unwrap();
                    onpage.extend(&op.page[4..]);
                    next = u32::from_be_bytes(op.page[..4].try_into().unwrap());
                }
                buf = &onpage;
                i = 0;
            } else if K > X {
                // the first M bytes of P are stored on the btree page and the remaining P-M bytes are stored on overflow pages.
                onpage = buf[i..i + M].to_vec();
                let mut next = u32::from_be_bytes(buf[i + M..i + M + 4].try_into().unwrap());
                while next != 0 {
                    let op = parse_page(next as usize - 1, reader, &db, true).unwrap();
                    onpage.extend(&op.page[4..]);
                    next = u32::from_be_bytes(op.page[..4].try_into().unwrap());
                }
                buf = &onpage;
                i = 0;
            } else {
                unreachable!();
            }

            // payload
            // decode record header
            let (header_size, j) = decode_varint(&buf[i..]);
            i += j;
            let mut serial_size = header_size as usize - j;
            let mut serials = Vec::new();
            while serial_size > 0 {
                let (serial_type, j) = decode_varint(&buf[i..]);
                i += j;
                serial_size -= j;
                serials.push(serial_type);
            }
            assert_eq!(serial_size, 0);

            // decode record body
            for (f, t) in serials.into_iter().enumerate() {
                let size = serial_type_size(t);
                let v = col_value(t, buf, i);
                i += size;
                state.on_col(ic, f, &v);
            }
            state.on_row();
        } else if p.page_type == 0x05 {
            let left = u32::from_be_bytes(buf[i..i + 4].try_into().unwrap());
            i += 4;
            let left_page = parse_page(left as usize - 1, reader, &db, false).unwrap();
            let (rowid, j) = decode_varint(&buf[i..]);
            i += j;
            parse_cell_as_rows(&left_page, state, reader, db);
            // eprintln!("0x05 interior key/rowid: {rowid}");
        } else {
            unimplemented!("parse cell for {}", p.page_type);
        }
    }

    if p.page_type == 0x05 {
        let right_page = parse_page(p.right.unwrap() as usize - 1, reader, &db, false).unwrap();
        parse_cell_as_rows(&right_page, state, reader, db);
    }
    state.finalize();
}

impl<'r> Tables<'r> {
    fn new(db: &DBInfo, p: &Page, reader: &'r File) -> Option<Self> {
        let mut res = Tables {
            dbinfo: *db,
            reader: reader,
            display: String::new(),
            pos: HashMap::new(),
            content: HashMap::new(),
            cur_tbl_name: String::new(),
            cur_rootpage: 0,
            cur_create: Default::default(),
        };

        parse_cell_as_rows(p, &mut res, reader, *db);
        // eprintln!("table: {:?}", res);
        return Some(res);
    }

    fn select(
        &self,
        table: &String,
        cols: Vec<String>,
        conditions: Vec<parser::Condition>,
    ) -> Result<()> {
        eprintln!("conds: {:?}", conditions);
        let t = self
            .content
            .get(table)
            .expect(&format!("cannot find table: {table}"));
        let rootpage = self
            .pos
            .get(table)
            .expect(&format!("cannot find table: {table}"));
        let p = parse_page(rootpage - 1, self.reader, &self.dbinfo, false).expect(&format!(
            "cannot parse page {} for table: {}",
            rootpage, table
        ));
        let mut indices = Vec::new();
        let len = cols.len();
        for col_name in cols {
            let col_index = t
                .columns
                .iter()
                .enumerate()
                .find(|c| c.1.name == col_name)
                .context(format!(
                    "cannot find column {} for table: {}",
                    col_name, table
                ))?;
            indices.push((col_index.0, col_name));
        }
        // eprintln!("create {:?}, indices:{:?}", t.columns, indices);
        let mut cp = ColsPrint {
            col_indices: indices,
            per_row: vec!["".to_string(); len],
            filtered: false,
            conditions: conditions,
            cur_type: 0,
        };
        parse_cell_as_rows(&p, &mut cp, self.reader, self.dbinfo);

        Ok(())
    }
}

struct MockCol;
impl OnColumn for MockCol {
    fn on_col(&mut self, row: usize, col: usize, v: &ColType) {
        eprintln!("on_col {row}, {col}, {v}");
    }

    fn on_row(&mut self) {
        eprintln!("on_row");
    }

    fn finalize(&mut self) {}

    fn set_type(&mut self, t: u8) {}
}

struct ColsPrint {
    col_indices: Vec<(usize, String)>,
    per_row: Vec<String>,
    filtered: bool,
    conditions: Vec<parser::Condition>,
    cur_type: u8,
}

impl OnColumn for ColsPrint {
    fn on_col(&mut self, row: usize, col: usize, v: &ColType) {
        // eprintln!("on_col: 0x{:0x}, {}, {}, {}", self.cur_type, row, col, v);
        // [3,1,2]
        // [1,2,3]
        // stored: name, color
        // select: color name
        // select name from xxx where color = 'Yellow';
        // TODO: We only support AND for now.
        if self.cur_type == 0x0d {
            if let Some((i, col)) = self
                .col_indices
                .iter()
                .enumerate()
                .find(|c| (*c.1).0 == col)
            {
                for cond in &self.conditions {
                    assert_eq!(cond.op, "=");
                    eprintln!(
                        "col: {} - {} - {}, expected: {}",
                        col.1,
                        cond.column,
                        v.to_string(),
                        cond.value,
                    );
                    if col.1 == cond.column && v.to_string() != cond.value {
                        self.filtered = true;
                        break;
                    }
                }
                self.per_row[i] = v.to_string();
            }
        }
    }

    fn on_row(&mut self) {
        if self.cur_type == 0x0d {
            if !self.filtered {
                println!("{}", self.per_row.join("|"));
            }
            self.per_row.resize(self.per_row.len(), "".to_string());
            self.filtered = false;
        }
    }

    fn finalize(&mut self) {}

    fn set_type(&mut self, t: u8) {
        eprintln!("set type: {}", t);
        self.cur_type = t
    }
}

#[derive(Debug, Copy, Clone)]
struct DBInfo {
    page_size: u16,
    text_encoding: u32,
    table_count: usize,
}

struct Page {
    page_type: u8,
    _freeblock_start: u16,
    cell_num: u16,
    cell_content_area: u16,
    page: Vec<u8>,

    cell_offsets: Vec<u16>,

    right: Option<u32>,
}

fn parse_dbinfo(reader: &mut File) -> Result<DBInfo> {
    let mut header = [0; 100];
    reader.seek(SeekFrom::Start(0))?;
    reader.read_exact(&mut header)?;
    let text_encoding = u32::from_be_bytes(header[56..60].try_into().unwrap());
    if text_encoding != 1 {
        panic!("unsupported text encoding {}", text_encoding);
    }
    assert_eq!(header[20], 0); // Bytes of unused "reserved" space at the end of each page. Usually 0. 

    // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
    #[allow(unused_variables)]
    let page_size = u16::from_be_bytes([header[16], header[17]]);
    let mut db = DBInfo {
        page_size,
        text_encoding,
        table_count: 0,
    };

    let page = parse_page(0, reader, &mut db, false)?;
    db.table_count = page.cell_num as usize;

    Ok(db)
}

fn parse_page<'r>(
    idx: usize,
    mut reader: &'r File,
    dbinfo: &DBInfo,
    overflow: bool,
) -> Result<Page> {
    let page_size = dbinfo.page_size as usize;
    let offset = idx * page_size;
    let mut page = vec![0; page_size];
    reader.seek(SeekFrom::Start(offset as u64))?;
    reader.read_exact(&mut page)?;
    if overflow {
        return Ok(Page {
            page_type: 0,
            _freeblock_start: 0,
            cell_num: 0,
            cell_content_area: 0,
            page: page,
            cell_offsets: Vec::new(),
            right: None,
        });
    }

    let page_header = if idx == 0 {
        &page[100..108]
    } else {
        &page[0..12]
    };

    let page_after_fh = if idx == 0 { &page[100..] } else { &page };

    let page_type = page_header[0];
    assert!(
        page_type == 0x0d || page_type == 0x05,
        "we only support leaf page now, but got {page_type}"
    );
    let is_leaf = page_type == 0x0d || page_type == 0x0a;
    let freeblock_start = u16::from_be_bytes(page_header[1..3].try_into().unwrap());
    let cell_num = u16::from_be_bytes(page_header[3..5].try_into().unwrap());
    let cell_content_area = u16::from_be_bytes(page_header[5..7].try_into().unwrap());
    let mut cell_offsets = Vec::new();
    let mut i = if is_leaf { 8 } else { 12 };
    let right = if !is_leaf {
        Some(u32::from_be_bytes(page_header[8..12].try_into().unwrap()))
    } else {
        None
    };
    for _ in 0..cell_num {
        cell_offsets.push(u16::from_be_bytes(
            page_after_fh[i..i + 2].try_into().unwrap(),
        ));
        i += 2;
    }

    let p = Page {
        page_type,
        _freeblock_start: freeblock_start,
        cell_num,
        cell_content_area,
        cell_offsets,
        page,
        right,
    };
    return Ok(p);
}

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    let mut file = File::open(&args[1])?;

    match command.as_str() {
        ".dbinfo" => {
            let db = parse_dbinfo(&mut file)?;
            println!("database page size: {}", db.page_size);
            println!("number of tables: {}", db.table_count);
        }
        ".tables" => {
            let db = parse_dbinfo(&mut file)?;
            let p = parse_page(0, &mut file, &db, false)?;
            let t = Tables::new(&db, &p, &mut file).expect("not getting legal tables");
            println!("{}", t.display);
        }
        statement if !statement.starts_with(".") => {
            let select = parser::parse_select(statement).expect("parse select err");
            // eprintln!("select: {select:?}");
            let table = select.table;
            let db = parse_dbinfo(&mut file)?;
            let p = parse_page(0, &mut file, &db, false)?;
            let t = Tables::new(&db, &p, &mut file).expect("not getting legal tables");
            t.select(&table, select.columns, select.conditions)
                .unwrap_or_else(|_| {
                    let root = t.pos.get(&table).expect(&format!("{} not exists", table));
                    let p = parse_page(*root - 1, &mut file, &db, false)
                        .context("parse page err")
                        .unwrap();
                    println!("{}", p.cell_num);
                });
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

#[derive(Debug)]
enum ColType {
    Null,
    Integer(i64),
    Float(f64),
    Reserved,
    Blob(usize),
    Text(String),
}

impl fmt::Display for ColType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColType::Null => write!(f, "NULL"),
            ColType::Integer(v) => write!(f, "{v}"),
            ColType::Float(v) => write!(f, "{v}"),
            ColType::Reserved => write!(f, "RESERVED"),
            ColType::Blob(size) => write!(f, "BLOB({size})"),
            ColType::Text(s) => write!(f, "{}", s),
        }
    }
}

fn col_value(serial_type: i64, buf: &[u8], start: usize) -> ColType {
    match serial_type {
        0 => ColType::Null,
        1 => ColType::Integer(buf[start] as i64),
        2 => ColType::Integer(((buf[start] as i64) << 8) + buf[start + 1] as i64),
        3 => ColType::Integer(
            ((buf[start] as i64) << 16) + ((buf[start + 1] as i64) << 8) + buf[start + 2] as i64,
        ),
        4 => ColType::Integer(
            ((buf[start] as i64) << 24)
                + ((buf[start + 1] as i64) << 16)
                + ((buf[start + 2] as i64) << 8)
                + buf[start + 3] as i64,
        ),
        5 => ColType::Integer(i64::from_be_bytes(
            buf[start..start + 6].try_into().unwrap(),
        )),
        6 => ColType::Integer(i64::from_be_bytes(
            buf[start..start + 8].try_into().unwrap(),
        )),
        7 => ColType::Float(f64::from_be_bytes(
            buf[start..start + 8].try_into().unwrap(),
        )), // 64-bit floating pointer
        8 => ColType::Integer(0),
        9 => ColType::Integer(0),
        10 | 11 => unimplemented!(),
        n if n >= 12 && n % 2 == 0 => ColType::Blob((n as usize - 12) / 2), // BLOB
        n if n >= 13 && n % 2 == 1 => ColType::Text(
            String::from_utf8(buf[start..(start + (n as usize - 13) / 2)].to_vec()).unwrap(),
        ), // TEXT: ensure text_encoding == 1
        other => panic!("unreachable: {}", other),
    }
}

fn serial_type_size(serial_type: i64) -> usize {
    match serial_type {
        0 => 0,
        1 => 1,
        2 => 2,
        3 => 3,
        4 => 4,
        5 => 6,
        6 => 8,
        7 => 8, // 64-bit floating pointer
        8 => 0,
        9 => 0,
        10 | 11 => unimplemented!(),
        n if n >= 12 && n % 2 == 0 => (n as usize - 12) / 2, // BLOB
        n if n >= 13 && n % 2 == 1 => (n as usize - 13) / 2, // TEXT
        other => panic!("unreachable: {}", other),
    }
}

fn decode_varint(buf: &[u8]) -> (i64, usize) {
    let mut i = 0;
    let mut res: i64 = 0;
    while i < 9 && i < buf.len() {
        i += 1;
        res = (res << 7) + (buf[i - 1] & 0x7F) as i64;
        if buf[i - 1] & 0x80 == 0 {
            break;
        }
    }
    (res, i)
}

#[test]
fn test_decode_varint() {
    assert_eq!(decode_varint(&[0x78]), (120, 1));
    assert_eq!(decode_varint(&[0x07]), (7, 1));
    assert_eq!(decode_varint(&[0x17]), (23, 1));
    assert_eq!(decode_varint(&[0x1b]), (27, 1));
    assert_eq!(decode_varint(&[0x81, 0x47]), (199, 2));
}
