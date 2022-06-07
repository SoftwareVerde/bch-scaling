package com.softwareverde.bitcoin.scaling.generate;

import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.scaling.DiskUtil;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.logging.Logger;

import java.io.File;

public class FanInBlockTransactionGenerator extends TransactionGenerator {
    protected long _startingBlockHeightToSpend;

    public FanInBlockTransactionGenerator(final PrivateKeyGenerator privateKeyGenerator, final File scenarioDirectory, final int coinbaseMaturityBlockCount, final long startingBlockHeightToSpend) {
        super(privateKeyGenerator, scenarioDirectory, coinbaseMaturityBlockCount);
        _startingBlockHeightToSpend = startingBlockHeightToSpend;
    }

    @Override
    public List<Transaction> getTransactions(final Long blockHeight, final List<BlockHeader> existingBlockHeaders, final List<BlockHeader> createdBlocks) {
        final MutableList<TransactionWithBlockHeight> transactionsToSpend;
        {
            final long blockHeightToSpend = blockHeight + _startingBlockHeightToSpend;
            final Sha256Hash blockHash = existingBlockHeaders.get((int) blockHeightToSpend).getHash();
            final Block blockToSpend = DiskUtil.loadBlock(blockHash, _scenarioDirectory);
            final int transactionCount = blockToSpend.getTransactionCount();
            transactionsToSpend = new MutableList<>(transactionCount - 1);

            final List<Transaction> transactions = blockToSpend.getTransactions();
            for (int i = 1; i < transactionCount; ++i) {
                final Transaction transaction = transactions.get(i);
                transactionsToSpend.add(new TransactionWithBlockHeight(transaction, blockHeightToSpend));
            }
        }

        try {
            final List<Transaction> transactions = GenerationUtil.createFanInTransactions(transactionsToSpend, blockHeight);
            _writeTransactionGenerationOrder(null, transactions, _scenarioDirectory, blockHeight);
            return transactions;
        }
        catch (final Exception exception) {
            Logger.warn(exception);
            return null;
        }
    }
}