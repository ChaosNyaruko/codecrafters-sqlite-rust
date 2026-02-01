use anyhow::{Result, bail};
use std::fmt::Write;
use std::fs::File;
use std::io::prelude::*;

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
    let mut header = [0; 100];
    file.read_exact(&mut header)?;
    let text_encoding = u32::from_be_bytes(header[56..60].try_into().unwrap());
    if text_encoding != 1 {
        panic!("unsupported text encoding {}", text_encoding);
    }

    // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
    #[allow(unused_variables)]
    let page_size = u16::from_be_bytes([header[16], header[17]]);

    let mut page_after_fh = vec![0; page_size as usize - 100];
    file.read_exact(&mut page_after_fh)?;

    let page_header = &page_after_fh[0..8]; // 8+4: TODO: 4 is for interior b-tree pages
    let page_type = page_header[0];
    let _freeblock_start = &page_header[1..3];
    let cell_num = u16::from_be_bytes(page_header[3..5].try_into().unwrap());

    match command.as_str() {
        ".dbinfo" => {
            println!("database page size: {}", page_size);
            println!("number of tables: {cell_num}");

            let cell_content_area = u16::from_be_bytes(page_header[5..7].try_into().unwrap());
            eprintln!("cell_content_area: {cell_content_area}");

            if page_type != 0x0a && page_type != 0x0d {
                panic!("{}", format!("unsupported page type: {}", page_type));
            }
        }
        ".tables" => {
            let mut res = "".to_string();

            let mut cell_offsets = Vec::new();
            let mut i = 8; // TODO: interior offset: 4
            for _ in 0..cell_num {
                cell_offsets.push(u16::from_be_bytes(
                    page_after_fh[i..i + 2].try_into().unwrap(),
                ));
                i += 2;
            }
            // eprintln!("{cell_offsets:?}");

            for (ic, offset) in cell_offsets.into_iter().enumerate() {
                let mut i = 0;
                let buf = &page_after_fh[offset as usize - 100..];
                let (size, j) = decode_varint(buf);
                i += j;
                let (rowid, j) = decode_varint(&buf[i..]);
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
                // type name tbl_name rootpage sql
                for (f, t) in serials.into_iter().enumerate() {
                    let size = serial_type_size(t);
                    let v = col_value(t, buf, i);
                    i += size;
                    if f == 2 {
                        if let ColType::Text(v) = v {
                            write!(res, "{}", v).unwrap();
                        }
                        if ic != cell_num as usize - 1 {
                            write!(res, " ").unwrap();
                        }
                    }
                }
            }

            println!("{}", res);
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
