/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.engine;

import com.arcadedb.database.Binary;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * Compression implementation that uses the popular LZ4 algorithm.
 */
public class LZ4Compression implements Compression {
  private static final byte[]              EMPTY_BYTES  = new byte[0];
  private static final Binary              EMPTY_BINARY = new Binary(EMPTY_BYTES);
  private final        LZ4Factory          factory;
  private final        LZ4Compressor       compressor;
  private final        LZ4FastDecompressor decompressor;

  public LZ4Compression() {
    this.factory = LZ4Factory.fastestInstance();
    this.compressor = factory.fastCompressor();
    this.decompressor = factory.fastDecompressor();
  }

  @Override
  public Binary compress(final Binary data) {
    final int decompressedLength = data.size() - data.position();
    final int maxCompressedLength = compressor.maxCompressedLength(decompressedLength);
    final byte[] compressed = new byte[maxCompressedLength];
    final int compressedLength = compressor
        .compress(data.getContent(), data.position(), data.size(), compressed, 0, maxCompressedLength);

    return new Binary(compressed, compressedLength);
  }

  @Override
  public Binary decompress(final Binary data, final int decompressedLength) {
    if (decompressedLength == 0)
      return EMPTY_BINARY;

    final int compressedLength = data.size() - data.position();
    if (compressedLength == 0)
      return EMPTY_BINARY;

    final byte[] decompressed = new byte[decompressedLength];
    decompressor.decompress(data.getContent(), data.position(), decompressed, 0, decompressedLength);
    return new Binary(decompressed);
  }
}
