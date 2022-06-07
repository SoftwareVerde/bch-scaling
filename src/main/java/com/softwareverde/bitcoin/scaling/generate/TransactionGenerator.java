package com.softwareverde.bitcoin.scaling.generate;

import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.constable.list.List;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.logging.Logger;

import java.io.File;
import java.io.FileOutputStream;

public abstract class TransactionGenerator {
    protected final PrivateKeyGenerator _privateKeyGenerator;
    protected final File _scenarioDirectory;
    protected final int _coinbaseMaturityBlockCount;

    public TransactionGenerator(final PrivateKeyGenerator privateKeyGenerator, final File scenarioDirectory, final int coinbaseMaturityBlockCount) {
        _privateKeyGenerator = privateKeyGenerator;
        _scenarioDirectory = scenarioDirectory;
        _coinbaseMaturityBlockCount = coinbaseMaturityBlockCount;
    }

    public abstract List<Transaction> getTransactions(Long blockHeight, List<BlockHeader> existingBlockHeaders, List<BlockHeader> newBlockHeaders);

    protected void _writeTransactionGenerationOrder(final Transaction transaction, final List<Transaction> transactions, final File _scenarioDirectory, final Long blockHeight) {
        final File directory = new File(_scenarioDirectory, "mempool");
        if (! directory.exists()) {
            directory.mkdirs();
        }

        final File file = new File(directory, blockHeight + ".sha");
        try (final FileOutputStream fileOutputStream = new FileOutputStream(file, false)) {
            if (transaction != null) {
                final Sha256Hash transactionHash = transaction.getHash();
                fileOutputStream.write(transactionHash.getBytes());
            }
            for (final Transaction tx : transactions) {
                final Sha256Hash transactionHash = tx.getHash();
                fileOutputStream.write(transactionHash.getBytes());
            }
        }
        catch (final Exception exception) {
            Logger.debug(exception);
        }
    }
}