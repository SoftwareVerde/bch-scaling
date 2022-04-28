package com.softwareverde.bitcoin.scaling;

import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockInflater;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.header.BlockHeaderInflater;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.constable.bytearray.MutableByteArray;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.logging.Logger;
import com.softwareverde.util.IoUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

public class DiskUtil {
    protected DiskUtil() { }

    public static Block loadBlock(final Sha256Hash blockHash, final File blocksDirectory) {
        final BlockInflater blockInflater = new BlockInflater();

        final File blockFile = new File(blocksDirectory, blockHash.toString());
        final ByteArray bytes = ByteArray.wrap(IoUtil.getFileContents(blockFile));
        return blockInflater.fromBytes(bytes);
    }

    public static BlockHeader loadBlockHeader(final Sha256Hash blockHash, final File blocksDirectory) {
        final BlockHeaderInflater blockHeaderInflater = new BlockHeaderInflater();

        final String blockHashString = blockHash.toString();
        final File blockFile = new File(blocksDirectory, blockHashString);

        try (final InputStream inputStream = new FileInputStream(blockFile)) {
            final MutableByteArray buffer = new MutableByteArray(BlockHeaderInflater.BLOCK_HEADER_BYTE_COUNT);
            IoUtil.readBytesFromStream(inputStream, buffer.unwrap());

            return blockHeaderInflater.fromBytes(buffer);
        }
        catch (final Exception exception) {
            Logger.debug(exception);
            return null;
        }
    }
}
