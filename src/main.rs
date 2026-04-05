use anyhow::{Context, Result, bail};
use std::collections::HashMap;
use std::fmt::{self, Write};
use std::fs::File;
use std::io::{SeekFrom, prelude::*};
mod parser;

#[derive(Debug, Clone)]
enum SelectBy {
    Conditions(Vec<parser::Condition>),
    RowIds(Vec<usize>),
}

#[derive(Debug, Clone)]
enum Create {
    Table(parser::CreateTableStmt),
    Index(parser::CreateIndexStmt),
    Null,
}

#[derive(Debug)]
struct Tables<'r> {
    dbinfo: DBInfo,
    reader: &'r File,

    // state
    cur_tbl_name: String,
    cur_name: String,
    cur_rootpage: usize,
    cur_create: Create,
    create_type: String,

    display: String,
    pos: HashMap<String, usize>,      // key: name, value: rootpage
    content: HashMap<String, Create>, // key: name, value: Table with column names
    // TODO: we only support one index per table
    indexes: HashMap<String, (String, String)>, // key: tbl_name,
                                                // value: (col_name,  index_name/name)
}

trait OnColumn {
    fn on_col(&mut self, cur_type: u8, row: usize, col: usize, v: &ColType, rowid: i64);
    fn on_row(&mut self, cur_type: u8, rowid: i64);
    fn finalize(&mut self);
}

impl<'r> OnColumn for Tables<'r> {
    fn on_col(&mut self, cur_type: u8, row: usize, col: usize, v: &ColType, rowid: i64) {
        // schema: type name tbl_name rootpage sql
        if col == 0 {
            self.create_type = v.to_string()
        }
        if col == 1 {
            self.cur_name = v.to_string()
        }
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
                eprintln!("sql:{}", sql);
                let cols = if self.create_type == "index" {
                    let c = parser::parse_create_index(&sql)
                        .expect(&format!("parse create table err: {sql}"));
                    assert_eq!(
                        c.columns.len(),
                        1,
                        "we only support single column index for now."
                    );
                    Create::Index(c)
                } else {
                    Create::Table(
                        parser::parse_create(&sql)
                            .expect(&format!("parse create table err: {sql}")),
                    )
                };
                self.cur_create = cols;
            }
        }
    }

    fn on_row(&mut self, _: u8, _rowid: i64) {
        eprintln!(
            "cur_name:{}, cur_create:{:?}",
            self.cur_name, self.cur_create
        );
        self.pos.insert(self.cur_name.clone(), self.cur_rootpage);
        self.content
            .insert(self.cur_name.clone(), self.cur_create.clone());
        if self.create_type == "index" {
            let i = match &self.cur_create {
                Create::Index(i) => i,
                _ => unreachable!(),
            };
            assert_eq!(self.cur_tbl_name, i.table);
            self.indexes.insert(
                self.cur_tbl_name.clone(),
                (i.columns[0].clone(), i.name.clone()),
            );
        }
    }

    fn finalize(&mut self) {}
}

// scan_btree sometimes returns the found rowids, when the page type is leaf index (0x0a)
// bad abstractions, but we are just demonstrating...
fn scan_btree(
    p: &Page,
    state: &mut dyn OnColumn,
    reader: &File,
    db: DBInfo,
    index_cond: Option<&parser::Condition>,
    rowid: Option<usize>,
) -> Vec<usize> {
    let cell_offsets = &p.cell_offsets;

    if p.page_type == 0x0d || p.page_type == 0x05 {
        // table nodes
        if rowid.is_none() {
            // preorder traversal for full scan
            for (ic, offset) in cell_offsets.into_iter().enumerate() {
                let (key, left) = parse_one_cell(ic, *offset, p, state, reader, db);
                state.on_row(p.page_type, -1);
                if left > 0 {
                    assert!(p.page_type == 0x02 || p.page_type == 0x05);
                    // only for interior nodes
                    let left_page = parse_page(left as usize - 1, reader, &db, false).unwrap();
                    scan_btree(&left_page, state, reader, db, index_cond, rowid);
                }
            }
            if p.page_type == 0x05 || p.page_type == 0x02 {
                let right_page =
                    parse_page(p.right.unwrap() as usize - 1, reader, &db, false).unwrap();
                scan_btree(&right_page, state, reader, db, index_cond, rowid);
            }
            state.finalize();
        } else {
            let rowid = rowid.unwrap();
            let target = rowid;
            if p.page_type == 0x05 {
                // interior
                let mut l = 0;
                let mut r = cell_offsets.len() - 1;
                while l < r {
                    let m = l + (r - l) / 2;
                    let (key, left) = parse_one_cell(m, cell_offsets[m], p, state, reader, db);
                    let key: usize = key.try_into().unwrap();
                    eprintln!("searching table 0x05 by rowid: {rowid} vs {key}, left:{left}");
                    // find the min key that greater than or (equal to) target
                    // 1 2 3 5 5 5 6 8
                    //      4^
                    if key < target {
                        l = m + 1;
                    } else {
                        r = m;
                    }
                }
                assert_eq!(l, r);
                // NOTE: we may want avoid the potential re-parse.
                let (key, left) = parse_one_cell(l, cell_offsets[l], p, state, reader, db);
                let key: usize = key.try_into().unwrap();
                state.on_row(p.page_type, key as i64);
                let next = if target > key {
                    eprintln!(
                        "l: {}, len: {}, target {} > {}",
                        l,
                        cell_offsets.len(),
                        target,
                        key,
                    );
                    p.right.unwrap() as usize
                } else {
                    eprintln!(
                        "l: {}, len: {}, target {} <= {}",
                        l,
                        cell_offsets.len(),
                        target,
                        key
                    );
                    left
                };
                let next_page = parse_page(next - 1, reader, &db, false).unwrap();
                return scan_btree(&next_page, state, reader, db, index_cond, Some(rowid));
            } else {
                // leaf 0x0d
                let mut l = 0;
                let mut r = cell_offsets.len() - 1;
                // for dup, find from the "smallest"
                // 1 2 3 4 5 5 5 5 6
                while l < r {
                    let m = l + (r - l) / 2;
                    let (key, _) = parse_one_cell(m, cell_offsets[m], p, state, reader, db);
                    let key: usize = key.try_into().unwrap();
                    eprintln!("searching table leaf 0x0d by target: {target} vs {key}");
                    if key < target {
                        l = m + 1;
                    } else {
                        r = m;
                    }
                }
                assert_eq!(l, r);
                while l < cell_offsets.len() {
                    let (rowid, _) = parse_one_cell(l, cell_offsets[l], p, state, reader, db);
                    let key: usize = rowid.try_into().unwrap();
                    state.on_row(p.page_type, key as i64);
                    if key == target {
                        eprintln!("post searching table leaf 0x0d by target: {target} vs {key}");
                        l += 1;
                    } else {
                        break;
                    }
                }
            }
        }
    } else if p.page_type == 0x02 {
        // interior index
        // binary search
        let target = index_cond.unwrap().value.clone();
        // v = condition.value
        // (key, left)
        // v(target) <= key (left)
        let mut l = 0;
        let mut r = cell_offsets.len() - 1;
        while l < r {
            let m = l + (r - l) / 2;
            let (key, left) = parse_one_cell(m, cell_offsets[m], p, state, reader, db);
            // TODO: use string just for demo, we might want to
            // define our own cmp for ColType
            eprintln!("searching index 0x02 by target: {target} vs {key}, left:{left}");
            // find the min key that greater than or (equal to) target
            // 1 2 3 5 5 5 6 8
            //      4^
            if key.to_string() < target {
                l = m + 1;
            } else {
                r = m;
            }
        }
        assert_eq!(l, r);
        // NOTE: we may want avoid the potential re-parse.
        let (key, left) = parse_one_cell(l, cell_offsets[l], p, state, reader, db);
        let next = if target > key.to_string() {
            eprintln!(
                "l: {}, len: {}, target {} > {}",
                l,
                cell_offsets.len(),
                target,
                key,
            );
            p.right.unwrap() as usize
        } else {
            eprintln!(
                "l: {}, len: {}, target {} <= {}",
                l,
                cell_offsets.len(),
                target,
                key
            );
            left
        };
        let next_page = parse_page(next - 1, reader, &db, false).unwrap();
        return scan_btree(&next_page, state, reader, db, index_cond, rowid);
    } else if p.page_type == 0xa {
        let target = index_cond.unwrap().value.clone();
        // cell_offsets
        //     .iter()
        //     .enumerate()
        //     .map(|(ic, offset)| {
        //         let (key, left) = parse_one_cell(ic, *offset, p, state, reader, db);
        //         eprintln!("0x0a: target {target}: {key}, {left}")
        //     })
        //     .collect::<()>();

        // leaf index node
        let mut l = 0;
        let mut r = cell_offsets.len() - 1;
        // for dup, find from the "smallest"
        // 1 2 3 4 5 5 5 5 6
        while l < r {
            let m = l + (r - l) / 2;
            let (key, _) = parse_one_cell(m, cell_offsets[m], p, state, reader, db);
            // TODO: use string just for demo, we might want to
            // define our own cmp for ColType
            eprintln!("searching index 0x0a by target: {target} vs {key}");
            if key.to_string() < target {
                l = m + 1;
            } else {
                r = m;
            }
        }
        let mut rowids = vec![];
        while l < cell_offsets.len() {
            let (key, rowid) = parse_one_cell(l, cell_offsets[l], p, state, reader, db);
            if key.to_string() == target {
                l += 1;
                eprintln!("find one: {}, rowid: {rowid} for target {target}", key);
                rowids.push(rowid);
            } else {
                break;
            }
        }
        return rowids;
    } else {
        unreachable!();
    }

    return Vec::default();
}

// -> key/rowid
// -> the left_pointer
fn parse_one_cell(
    ic: usize,
    offset: u16,
    p: &Page,
    state: &mut dyn OnColumn,
    reader: &File,
    db: DBInfo,
) -> (ColType, usize) {
    let mut res = ColType::Null;
    let mut left: usize = 0;

    let page = &p.page;
    let mut buf = &page[offset as usize..];
    let mut i = 0;
    if p.page_type == 0x0d {
        let (size, j1) = decode_varint(buf);
        i += j1;
        let (rowid, j2) = decode_varint(&buf[i..]);
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
            state.on_col(p.page_type, ic, f, &v, rowid);
        }
        res = ColType::Integer(rowid);
    } else if p.page_type == 0x05 {
        let lefta = u32::from_be_bytes(buf[i..i + 4].try_into().unwrap());
        i += 4;
        let (rowid, j) = decode_varint(&buf[i..]);
        i += j;
        res = ColType::Integer(rowid);
        left = lefta as usize;
    } else if p.page_type == 0x02 {
        let lefta = u32::from_be_bytes(buf[i..i + 4].try_into().unwrap());
        i += 4;
        let (size, j1) = decode_varint(buf);
        i += j1;

        let U = db.page_size as usize;
        let X = ((U - 12) * 64 / 255) - 23;
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
            eprintln!("page type 0x02: {f}, value: {v}");
            if f == 0 {
                // for single column index:
                // 0: key value
                // 1: rowid
                // we don't support multi column index for now
                res = v.clone();
            }
            i += size;
            state.on_col(p.page_type, ic, f, &v, -1);
        }
        left = lefta as usize
    } else if p.page_type == 0x0a {
        // payload size
        let (size, j1) = decode_varint(buf);
        i += j1;

        // payload body with overflow pages
        let U = db.page_size as usize;
        let X = ((U - 12) * 64 / 255) - 23;
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

        let mut rowid = 0;
        // decode record body
        // NOTE: we only support one-column index.
        for (f, t) in serials.into_iter().enumerate() {
            let size = serial_type_size(t);
            let v = col_value(t, buf, i);
            eprintln!("page_type: 0x0a: {f}, value:{v}");
            if f == 0 {
                res = v.clone();
            }
            if f == 1 {
                rowid = match v {
                    ColType::Integer(vv) => vv as usize,
                    _ => {
                        panic!("rowid is an i64")
                    }
                };
            }
            i += size;
            state.on_col(p.page_type, ic, f, &v, -1);
        }
        left = rowid;
    } else {
        unreachable!("parse cell for {}", p.page_type);
    }

    return (res, left);
}

fn parse_cell_as_tables(p: &Page, state: &mut dyn OnColumn, reader: &File, db: DBInfo) {
    let cell_offsets = &p.cell_offsets;
    for (ic, offset) in cell_offsets.into_iter().enumerate() {
        parse_one_cell(ic, *offset, p, state, reader, db);
        state.on_row(p.page_type, -1);
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
            cur_name: String::new(),
            cur_rootpage: 0,
            cur_create: Create::Null,
            create_type: "table".to_string(),
            indexes: HashMap::new(),
        };

        parse_cell_as_tables(p, &mut res, reader, *db);
        // eprintln!("table: {:?}", res);
        return Some(res);
    }

    fn select_rowids_by_index(
        &self,
        index_name: &String,
        conditions: Vec<parser::Condition>,
    ) -> Result<Vec<usize>> {
        let index = self
            .content
            .get(index_name)
            .expect(&format!("cannot find table: {index_name}"));
        let index_rootpage = self
            .pos
            .get(index_name)
            .expect(&format!("cannot find table: {index_name}"));
        let p = parse_page(index_rootpage - 1, self.reader, &self.dbinfo, false).expect(&format!(
            "cannot parse page {} for table: {}",
            index_rootpage, index_name
        ));
        let t = match index {
            Create::Index(c) => c,
            _ => unimplemented!(),
        };

        // simple index optimizer
        // again, we only support one condition for now
        eprintln!(
            "cond: {:?}, t.columns: {:?}",
            conditions[0].column, t.columns
        );
        let test = t.columns.iter().find(|v| **v == conditions[0].column);
        eprintln!("test: {:?}", test);

        if conditions.len() == 1
            && t.columns
                .iter()
                .find(|v| **v == conditions[0].column)
                .is_some()
        {
            let mut cp = IndexCol {
                conditions: conditions.clone(),
            };
            let rowid = scan_btree(
                &p,
                &mut cp,
                self.reader,
                self.dbinfo,
                Some(&conditions[0]),
                None,
            );
            return Ok(rowid);
        } else {
            return Err(anyhow::anyhow!("no index usable"));
        }
    }

    fn select(&self, table: &String, cols: Vec<String>, select_by: SelectBy) -> Result<()> {
        let tables = self
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
        let t = match tables {
            Create::Table(c) => c,
            _ => unimplemented!(),
        };
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
        eprintln!("create {:?}, indices:{:?}", t.columns, indices);
        let mut cp = ColsPrint {
            select_indices: indices,
            schema: t.columns.clone(),
            per_row: vec!["".to_string(); len],
            filtered: false,
            select_by: select_by.clone(),
        };
        match select_by {
            SelectBy::Conditions(_) => {
                scan_btree(&p, &mut cp, self.reader, self.dbinfo, None, None);
            }
            SelectBy::RowIds(rowids) => {
                for rowid in rowids {
                    eprintln!("XXrowid : {:?}", rowid);
                    cp.select_by = SelectBy::RowIds(vec![rowid]);
                    scan_btree(&p, &mut cp, self.reader, self.dbinfo, None, Some(rowid));
                }
            }
        }
        Ok(())
    }
}

struct MockCol;
impl OnColumn for MockCol {
    fn on_col(&mut self, _: u8, row: usize, col: usize, v: &ColType, rowid: i64) {
        eprintln!("on_col {row}, {col}, {v}");
    }

    fn on_row(&mut self, _: u8, _: i64) {
        eprintln!("on_row");
    }

    fn finalize(&mut self) {}
}

struct IndexCol {
    conditions: Vec<parser::Condition>,
}

impl OnColumn for IndexCol {
    fn on_col(&mut self, cur_type: u8, row: usize, col: usize, v: &ColType, rowid: i64) {
        eprintln!("on_col {row}, {col}, {v}");
    }

    fn on_row(&mut self, cur_type: u8, _: i64) {
        eprintln!("on_row");
    }

    fn finalize(&mut self) {}
}

struct ColsPrint {
    select_indices: Vec<(usize, String)>,
    schema: Vec<parser::ColumnDef>,
    per_row: Vec<String>,
    filtered: bool,
    select_by: SelectBy,
}

impl OnColumn for ColsPrint {
    fn on_col(&mut self, cur_type: u8, row: usize, col: usize, rv: &ColType, rowid: i64) {
        let v = if let ColType::Null = rv {
            &ColType::Integer(rowid)
        } else {
            rv
        };
        eprintln!(
            "on_col: 0x{:0x}, {}, row: {}, col: {}, rowid: {}",
            cur_type, row, col, v, rowid
        );
        if cur_type == 0x0d {
            // [3,1,2]
            // [1,2,3]
            // stored: name, color
            // select: color name
            // select name from xxx where color = 'Yellow';
            // TODO: We only support AND for now.
            match &self.select_by {
                SelectBy::Conditions(conditions) => {
                    for cond in conditions {
                        assert_eq!(cond.op, "=");
                        let c = self
                            .schema
                            .iter()
                            .enumerate()
                            .find(|c| c.1.name == cond.column)
                            .expect(&format!("cannot find the condtion {}", cond.column));
                        if c.0 != col {
                            continue;
                        }
                        eprintln!(
                            "{} vs {}: {} vs {}",
                            cond.column,
                            c.1.name,
                            cond.value,
                            v.to_string()
                        );
                        if v.to_string() != cond.value {
                            self.filtered = true;
                            break;
                        }
                    }
                }
                SelectBy::RowIds(_) => {
                    // NOTE: we do nothing here, we do the filter at on_row,
                    // to avoid re-assgining ".filter" and messing up.
                }
            }
            if let Some((i, col)) = self
                .select_indices
                .iter()
                .enumerate()
                .find(|c| (*c.1).0 == col)
            {
                self.per_row[i] = v.to_string();
            }
        }
    }

    fn on_row(&mut self, cur_type: u8, rowid: i64) {
        if cur_type == 0x0d {
            eprintln!(
                "0x0d search: {:?}, filterd: {:?}, per_row: {:?}",
                self.select_by, self.filtered, self.per_row
            );
            match &self.select_by {
                SelectBy::RowIds(rowids) => {
                    assert_eq!(rowids.len(), 1);
                    let target = rowids[0];
                    eprintln!("on_col search filter {target} vs {rowid}");
                    if target != rowid as usize {
                        self.filtered = true
                    }
                }
                _ => {}
            }
            if !self.filtered {
                println!("{}", self.per_row.join("|"));
            }
            self.per_row.resize(self.per_row.len(), "".to_string());
            self.filtered = false;
        }
        self.filtered = false;
    }

    fn finalize(&mut self) {}
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
        page_type == 0x0d || page_type == 0x05 || page_type == 0x02 || page_type == 0x0a,
        "invalid page_type {page_type}"
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
    // assert!("open" <= "one-side");
    // panic!();
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
            let tables = Tables::new(&db, &p, &mut file).expect("not getting legal tables");
            // assert_eq!(select.columns.len(), 1, "{:?}", select.columns);
            assert!(
                select.conditions.len() <= 1,
                "we only support single column index"
            );
            eprintln!(
                "indexes: {:?}, pos: {:?}, content: {:?}, table: {}",
                tables.indexes, tables.pos, tables.content, table
            );
            let rowids = if let Some(c) = tables.indexes.get(&table) {
                match tables.select_rowids_by_index(&c.1, select.conditions.clone()) {
                    Ok(rowids) => {
                        eprintln!("searching through index and get rowids: {:?}", rowids);
                        if rowids.len() == 0 {
                            // we can use index, don't find anything.
                            eprintln!("Don't find any items");
                            return Ok(());
                        } else {
                            Some(rowids)
                        }
                    }
                    Err(info) => {
                        // we have index on this table, but not on this particular column
                        eprintln!("{}", info);
                        None
                    }
                }
            } else {
                // we don't have index definitions on this table
                None
            };

            if rowids.is_some() {
                tables
                    .select(&table, select.columns, SelectBy::RowIds(rowids.unwrap()))
                    .expect("we must find some rows after we have rowids(through index)")
            } else {
                tables
                    .select(
                        &table,
                        select.columns,
                        SelectBy::Conditions(select.conditions),
                    )
                    .unwrap_or_else(|_| {
                        let root = tables
                            .pos
                            .get(&table)
                            .expect(&format!("{} not exists", table));
                        let p = parse_page(*root - 1, &mut file, &db, false)
                            .context("parse page err")
                            .unwrap();
                        println!("{}", p.cell_num);
                    });
            }
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

#[derive(Debug, Clone)]
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

impl TryFrom<ColType> for i64 {
    type Error = anyhow::Error;

    fn try_from(v: ColType) -> anyhow::Result<Self> {
        match v {
            ColType::Integer(n) => Ok(n),
            other => Err(anyhow::anyhow!("expected Integer, got {}", other)),
        }
    }
}

impl TryFrom<ColType> for usize {
    type Error = anyhow::Error;

    fn try_from(v: ColType) -> anyhow::Result<Self> {
        match v {
            ColType::Integer(n) => Ok(n as usize),
            other => Err(anyhow::anyhow!("expected Integer, got {}", other)),
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
