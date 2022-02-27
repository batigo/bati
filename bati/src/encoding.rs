use std::fmt;
use std::io::{Cursor, Read, Result as IOResult, Write};

pub const ZSTD_NAME: &str = "zstd";
pub const DEFLATE_NAME: &str = "deflate";
pub const NULLENCODER_NAME: &str = "null";

#[derive(Default, Clone, Copy)]
pub struct Zstd;

impl Zstd {
    fn compress(d: &[u8]) -> IOResult<Vec<u8>> {
        let buffer = Cursor::new(Vec::new());
        let mut encoder = zstd::stream::write::Encoder::new(buffer, 0)?;
        encoder.write_all(d)?;
        let data = encoder.finish()?;
        Ok(data.into_inner())
    }

    fn decompress(d: &[u8]) -> IOResult<Vec<u8>> {
        let buffer = Cursor::new(Vec::new());
        let mut decoder = zstd::stream::write::Decoder::new(buffer)?;
        decoder.write_all(d)?;
        decoder.flush()?;
        Ok(decoder.into_inner().into_inner())
    }
}

#[derive(Default, Clone, Copy)]
pub struct Deflater;

impl Deflater {
    fn compress(d: &[u8]) -> IOResult<Vec<u8>> {
        let mut w = flate2::write::DeflateEncoder::new(vec![], flate2::Compression::default());
        w.write_all(d)?;
        w.finish()
    }

    fn decompress(d: &[u8]) -> IOResult<Vec<u8>> {
        let mut r = flate2::read::DeflateDecoder::new(d);
        let mut bs = Vec::with_capacity(512);
        r.read_to_end(&mut bs)?;
        Ok(bs)
    }
}

#[derive(Default, Clone, Copy)]
pub struct Null {}
impl Null {
    fn compress(d: &[u8]) -> IOResult<Vec<u8>> {
        Ok(d.to_vec())
    }

    fn decompress(d: &[u8]) -> IOResult<Vec<u8>> {
        Ok(d.to_vec())
    }
}

#[derive(Clone, Debug)]
pub struct Encoder {
    name: &'static str,
}

impl fmt::Display for Encoder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "encode-{}", self.name)
    }
}

impl Encoder {
    pub fn new(name: &str) -> Self {
        match name {
            ZSTD_NAME => Encoder { name: ZSTD_NAME },
            DEFLATE_NAME => Encoder { name: DEFLATE_NAME },
            _ => Encoder {
                name: NULLENCODER_NAME,
            },
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn encode(&self, d: &[u8]) -> IOResult<Vec<u8>> {
        match self.name {
            ZSTD_NAME => Zstd::compress(d),
            DEFLATE_NAME => Deflater::compress(d),
            _ => Null::compress(d),
        }
    }

    pub fn decode(&self, d: &[u8]) -> IOResult<Vec<u8>> {
        match self.name {
            ZSTD_NAME => Zstd::decompress(d),
            DEFLATE_NAME => Deflater::decompress(d),
            _ => Null::decompress(d),
        }
    }
}

impl PartialEq for Encoder {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zstd() {
        let raw_s = "adjsdfas;1234324sdfsdfsdfasfasfs23uirelsjlewrpoeopsfs速度快发".to_string();
        let encoder = Encoder::new(ZSTD_NAME);
        let zip_data = encoder.encode(raw_s.as_bytes());
        assert!(zip_data.is_ok());
        let unzip_data = encoder.decode(zip_data.unwrap().as_slice());
        assert!(unzip_data.is_ok());
        assert_eq!(raw_s.into_bytes(), unzip_data.unwrap());
    }

    #[test]
    fn test_deflate() {
        let raw_s = "adjsdfas;1234324sdfsdfsdfasfasfs23uirelsjlewrpoeopsfs速度快发".to_string();
        let encoder = Encoder::new(DEFLATE_NAME);
        let zip_data = encoder.encode(raw_s.as_bytes());
        assert!(zip_data.is_ok());
        let unzip_data = encoder.decode(zip_data.unwrap().as_slice());
        assert!(unzip_data.is_ok());
        assert_eq!(raw_s.into_bytes(), unzip_data.unwrap());
    }
}
