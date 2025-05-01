use bytes::{BufMut, Bytes, BytesMut};
use cityhash_rs::cityhash_102_128;
use lz4_flex::block;

use crate::exporters::clickhouse::ch_error::{Error, Result};

// Meta = checksum + header
// - [16b] checksum
// - [ 1b] magic number (0x82)
// - [ 4b] compressed size (data + header)
// - [ 4b] uncompressed size
const LZ4_CHECKSUM_SIZE: usize = 16;
const LZ4_HEADER_SIZE: usize = 9;
const LZ4_META_SIZE: usize = LZ4_CHECKSUM_SIZE + LZ4_HEADER_SIZE;
const LZ4_MAGIC: u8 = 0x82;

struct Lz4Meta {
    checksum: u128,
    compressed_size: u32,
    uncompressed_size: u32,
}

impl Lz4Meta {
    fn write_checksum(&self, mut buffer: &mut [u8]) {
        buffer.put_u128_le(self.checksum);
    }

    fn write_header(&self, mut buffer: &mut [u8]) {
        buffer.put_u8(LZ4_MAGIC);
        buffer.put_u32_le(self.compressed_size);
        buffer.put_u32_le(self.uncompressed_size);
    }
}

fn calc_checksum(buffer: &[u8]) -> u128 {
    let hash = cityhash_102_128(buffer);
    hash.rotate_right(64)
}

pub(crate) fn compress(uncompressed: &[u8]) -> Result<Bytes> {
    let max_compressed_size = block::get_maximum_output_size(uncompressed.len());

    let mut buffer = BytesMut::new();
    buffer.resize(LZ4_META_SIZE + max_compressed_size, 0);

    let compressed_data_size = block::compress_into(uncompressed, &mut buffer[LZ4_META_SIZE..])
        .map_err(|err| Error::Compression(err.into()))?;

    buffer.truncate(LZ4_META_SIZE + compressed_data_size);

    let mut meta = Lz4Meta {
        checksum: 0, // will be calculated below.
        compressed_size: (LZ4_HEADER_SIZE + compressed_data_size) as u32,
        uncompressed_size: uncompressed.len() as u32,
    };

    meta.write_header(&mut buffer[LZ4_CHECKSUM_SIZE..]);
    meta.checksum = calc_checksum(&buffer[LZ4_CHECKSUM_SIZE..]);
    meta.write_checksum(&mut buffer[..]);

    Ok(buffer.freeze())
}

#[test]
fn it_compresses() {
    let source = vec![
        1u8, 0, 2, 255, 255, 255, 255, 0, 1, 1, 1, 115, 6, 83, 116, 114, 105, 110, 103, 3, 97, 98,
        99,
    ];

    let expected = vec![
        245_u8, 5, 222, 235, 225, 158, 59, 108, 225, 31, 65, 215, 66, 66, 36, 92, 130, 34, 0, 0, 0,
        23, 0, 0, 0, 240, 8, 1, 0, 2, 255, 255, 255, 255, 0, 1, 1, 1, 115, 6, 83, 116, 114, 105,
        110, 103, 3, 97, 98, 99,
    ];

    let actual = compress(&source).unwrap();
    assert_eq!(actual, expected);
}
