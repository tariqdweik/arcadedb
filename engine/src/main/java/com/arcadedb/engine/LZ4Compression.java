/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.engine;

import com.arcadedb.database.Binary;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

/**
 * Compression implementation that uses the popular LZ4 algorithm.
 */
public class LZ4Compression implements Compression {
  private final LZ4Factory          factory;
  private final LZ4Compressor       compressor;
  private final LZ4SafeDecompressor decompressor;

  public LZ4Compression() {
    this.factory = LZ4Factory.fastestInstance();
    this.compressor = factory.fastCompressor();
    this.decompressor = factory.safeDecompressor();
  }

  @Override
  public Binary compress(final Binary data) {
    final int maxCompressedLength = compressor.maxCompressedLength(data.size());
    final byte[] compressed = new byte[maxCompressedLength];
    final int compressedLength = compressor.compress(data.getContent(), 0, data.size(), compressed, 0, maxCompressedLength);

    com.arcadedb.utility.LogManager.instance()
        .info(this, "Compression buffer from %d to %d (%d%%)", data.size(), compressedLength, compressedLength * 100 / data.size());

    return new Binary(compressed, compressedLength);
  }

  @Override
  public Binary decompress(final Binary data) {
    final byte[] decompressed = new byte[data.size()];
    final int decompressedLength = decompressor.decompress(data.getContent(), 0, data.size(), decompressed, 0);
    return new Binary(decompressed, decompressedLength);
  }
}
