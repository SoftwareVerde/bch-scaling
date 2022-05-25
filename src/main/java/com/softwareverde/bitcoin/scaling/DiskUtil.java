package com.softwareverde.bitcoin.scaling;

import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockInflater;
import com.softwareverde.bitcoin.block.header.BlockHeaderInflater;
import com.softwareverde.bitcoin.block.header.BlockHeaderWithTransactionCount;
import com.softwareverde.bitcoin.block.header.BlockHeaderWithTransactionCountInflater;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.constable.bytearray.MutableByteArray;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.logging.Logger;
import com.softwareverde.util.ByteBuffer;
import com.softwareverde.util.IoUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.RandomAccessFile;

public class DiskUtil {
    protected DiskUtil() { }

    protected static final ByteBuffer BYTE_BUFFER = new ByteBuffer();

    public static ByteArray readFromBlock(final Sha256Hash blockHash, final Long diskOffset, final Integer byteCount, final File blocksDirectory) {
        final File blockFile = new File(blocksDirectory, blockHash.toString());

        if (! blockFile.exists()) { return null; }

        try {
            final MutableByteArray byteArray = new MutableByteArray(byteCount);

            try (final RandomAccessFile file = new RandomAccessFile(blockFile, "r")) {
                file.seek(diskOffset);

                final byte[] buffer;
                synchronized (BYTE_BUFFER) {
                    buffer = BYTE_BUFFER.getRecycledBuffer();
                }

                int totalBytesRead = 0;
                while (totalBytesRead < byteCount) {
                    final int byteCountRead = file.read(buffer);
                    if (byteCountRead < 0) { break; }

                    byteArray.setBytes(totalBytesRead, buffer);
                    totalBytesRead += byteCountRead;
                }

                synchronized (BYTE_BUFFER) {
                    BYTE_BUFFER.recycleBuffer(buffer);
                }

                if (totalBytesRead < byteCount) { return null; }
            }

            return byteArray;
        }
        catch (final Exception exception) {
            Logger.warn(exception);
            return null;
        }
    }

    public static Block loadBlock(final Sha256Hash blockHash, final File blocksDirectory) {
        final BlockInflater blockInflater = new BlockInflater();

        final File blockFile = new File(blocksDirectory, blockHash.toString());
        final ByteArray bytes = ByteArray.wrap(IoUtil.getFileContents(blockFile));
        return blockInflater.fromBytes(bytes);
    }

    public static BlockHeaderWithTransactionCount loadBlockHeader(final Sha256Hash blockHash, final File blocksDirectory) {
        final BlockHeaderWithTransactionCountInflater blockHeaderInflater = new BlockHeaderWithTransactionCountInflater();

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
