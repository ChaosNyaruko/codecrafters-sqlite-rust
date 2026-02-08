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
                let cols = parser::parse_create(&sql).expect(&format!("parse create err: {sql}"));
                // eprintln!("create: {cols:?}");
                self.cur_create = cols;
            }
        }
    }

    fn on_row(&mut self) {
        self.pos
            .insert(self.cur_tbl_name.clone(), self.cur_rootpage);
        assert_eq!(
            self.cur_tbl_name, self.cur_create.table,
            "create table name should be consistent with the tbl_name field"
        );
        self.content
            .insert(self.cur_tbl_name.clone(), self.cur_create.clone());
    }

    fn finalize(&mut self) {}
}

fn parse_cell_as_rows(p: &Page, state: &mut dyn OnColumn) {
    let page = &p.page;
    let cell_offsets = &p.cell_offsets;
    for (ic, offset) in cell_offsets.into_iter().enumerate() {
        let mut i = 0;
        let buf = &page[*offset as usize..];
        let (_size, j) = decode_varint(buf);
        i += j;
        let (_rowid, j) = decode_varint(&buf[i..]);
        i += j;

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

        parse_cell_as_rows(p, &mut res);
        // eprintln!("table: {:?}", res);
        return Some(res);
    }

    fn select(&self, table: &String, cols: Vec<String>) -> Result<()> {
        let t = self
            .content
            .get(table)
            .expect(&format!("cannot find table: {table}"));
        let rootpage = self
            .pos
            .get(table)
            .expect(&format!("cannot find table: {table}"));
        let p = parse_page(rootpage - 1, self.reader, &self.dbinfo).expect(&format!(
            "cannot parse page {} for table: {}",
            rootpage, table
        ));
        for col in cols {
            let col_index = t
                .columns
                .iter()
                .enumerate()
                .find(|c| c.1.name == col)
                .context(format!("cannot find column {} for table: {}", col, table))?;
            let mut cp = ColPrint {
                col_index: col_index.0,
            };
            parse_cell_as_rows(&p, &mut cp);
        }

        Ok(())
    }
}

struct ColPrint {
    col_index: usize,
}

impl OnColumn for ColPrint {
    fn on_col(&mut self, row: usize, col: usize, v: &ColType) {
        if col != self.col_index {
            return;
        }
        println!("{}", v);
    }

    fn on_row(&mut self) {}

    fn finalize(&mut self) {}
}

#[derive(Debug, Copy, Clone)]
struct DBInfo {
    page_size: u16,
    text_encoding: u32,
    table_count: usize,
}

struct Page {
    _page_type: u8,
    _freeblock_start: u16,
    cell_num: u16,
    cell_content_area: u16,
    page: Vec<u8>,

    cell_offsets: Vec<u16>,
}

fn parse_dbinfo(reader: &mut File) -> Result<DBInfo> {
    let mut header = [0; 100];
    reader.seek(SeekFrom::Start(0))?;
    reader.read_exact(&mut header)?;
    let text_encoding = u32::from_be_bytes(header[56..60].try_into().unwrap());
    if text_encoding != 1 {
        panic!("unsupported text encoding {}", text_encoding);
    }

    // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
    #[allow(unused_variables)]
    let page_size = u16::from_be_bytes([header[16], header[17]]);
    let mut db = DBInfo {
        page_size,
        text_encoding,
        table_count: 0,
    };

    let page = parse_page(0, reader, &mut db)?;
    db.table_count = page.cell_num as usize;

    Ok(db)
}

fn parse_page<'r>(idx: usize, mut reader: &'r File, dbinfo: &DBInfo) -> Result<Page> {
    let page_size = dbinfo.page_size as usize;
    let offset = idx * page_size;
    let mut page = vec![0; page_size];
    reader.seek(SeekFrom::Start(offset as u64))?;
    reader.read_exact(&mut page)?;

    let page_header = if idx == 0 {
        &page[100..108]
    } else {
        &page[0..8]
    };

    let page_after_fh = if idx == 0 { &page[100..] } else { &page };

    let page_type = page_header[0];
    assert!(
        page_type == 0x0a || page_type == 0x0d,
        "we only support leaf page now"
    );
    let freeblock_start = u16::from_be_bytes(page_header[1..3].try_into().unwrap());
    let cell_num = u16::from_be_bytes(page_header[3..5].try_into().unwrap());
    let cell_content_area = u16::from_be_bytes(page_header[5..7].try_into().unwrap());
    let mut cell_offsets = Vec::new();
    let mut i = 8; // TODO: interior offset: 4, has been asserted in header parsing.
    for _ in 0..cell_num {
        cell_offsets.push(u16::from_be_bytes(
            page_after_fh[i..i + 2].try_into().unwrap(),
        ));
        i += 2;
    }

    let p = Page {
        _page_type: page_type,
        _freeblock_start: freeblock_start,
        cell_num,
        cell_content_area,
        cell_offsets,
        page,
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
            let p = parse_page(0, &mut file, &db)?;
            let t = Tables::new(&db, &p, &mut file).expect("not getting legal tables");
            println!("{}", t.display);
        }
        statement if !statement.starts_with(".") => {
            let select = parser::parse_select(statement).expect("parse select err");
            // eprintln!("select: {select:?}");
            let table = select.table;
            let db = parse_dbinfo(&mut file)?;
            let p = parse_page(0, &mut file, &db)?;
            let t = Tables::new(&db, &p, &mut file).expect("not getting legal tables");
            t.select(&table, select.columns).unwrap_or_else(|_| {
                let root = t.pos.get(&table).expect(&format!("{} not exists", table));
                let p = parse_page(*root - 1, &mut file, &db)
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
        2 => ColType::Integer(i64::from_be_bytes(
            buf[start..start + 2].try_into().unwrap(),
        )),
        3 => ColType::Integer(i64::from_be_bytes(
            buf[start..start + 3].try_into().unwrap(),
        )),
        4 => ColType::Integer(i64::from_be_bytes(
            buf[start..start + 4].try_into().unwrap(),
        )),
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
        6 => 7,
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
