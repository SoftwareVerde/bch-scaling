package com.softwareverde.bitcoin.scaling.generate;

import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.scaling.DiskUtil;
import com.softwareverde.bitcoin.scaling.Main;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.logging.Logger;

import java.io.File;

public class QuasiSteadyStateRound1BlockTransactionGenerator extends TransactionGenerator {
    protected long _startingBlockHeight;
    protected long _startingBlockHeightToSpend;

    public QuasiSteadyStateRound1BlockTransactionGenerator(PrivateKeyGenerator privateKeyGenerator, File scenarioDirectory, int coinbaseMaturityBlockCount, long startingBlockHeight, long startingBlockHeightToSpend) {
        super(privateKeyGenerator, scenarioDirectory, coinbaseMaturityBlockCount);
        _startingBlockHeight = startingBlockHeight;
        _startingBlockHeightToSpend = startingBlockHeightToSpend;
    }

    @Override
    public List<Transaction> getTransactions(final Long blockHeight, final List<BlockHeader> existingBlockHeaders, final List<BlockHeader> createdBlocks) {
        final MutableList<TransactionWithBlockHeight> transactionsToSpend = new MutableList<>();
        {
            final StringBuilder stringBuilder = new StringBuilder("blockHeight=" + blockHeight);
            // Spending only first half of each 10 fan-out blocks, over 5 quasi-steady-state blocks, means each quasi-steady state block spends 1/5 of 1/2 of each fan-out block.
            final int quasiSteadyStateBlockIndex = (int) (blockHeight - _startingBlockHeight);
            stringBuilder.append(" quasiSteadyStateBlockIndex=" + quasiSteadyStateBlockIndex);
            for (int i = 0; i < 10; ++i) {
                stringBuilder.append(" (");
                final long blockHeightToSpend = (_startingBlockHeightToSpend + i); // (firstFanOutBlockHeight + steadyStateBlockIndex + i)
                final Sha256Hash blockHash = Main.getBlockHash(blockHeightToSpend, existingBlockHeaders, createdBlocks);
                final Block blockToSpend = DiskUtil.loadBlock(blockHash, _scenarioDirectory);
                stringBuilder.append("blockHeightToSpend=" + blockHeightToSpend + " blockHash=" + blockHash);

                final int transactionCount = blockToSpend.getTransactionCount();
                stringBuilder.append(" transactionCount=" + transactionCount);
                final int transactionCountToSpend = ((transactionCount / 2) / 5); // (transactionCount / 5)
                stringBuilder.append(" transactionCountToSpend=" + transactionCountToSpend);
                final List<Transaction> transactions = blockToSpend.getTransactions();
                stringBuilder.append(" transactions.count=" + transactions.getCount());
                final int startIndex = (1 + (transactionCountToSpend * quasiSteadyStateBlockIndex));
                stringBuilder.append(" startIndex=" + startIndex);
                final int endIndex = (transactionCountToSpend + startIndex);
                for (int j = startIndex; j < endIndex; ++j) {
                    if (j >= transactionCount) { break; }

                    final Transaction transaction = transactions.get(j);
                    transactionsToSpend.add(new TransactionWithBlockHeight(transaction, blockHeightToSpend));
                }
                stringBuilder.append(")");
            }
            // Logger.debug(stringBuilder);
        }
        // Logger.debug("transactionsToSpend.count=" + transactionsToSpend.getCount());

        try {
            final List<Transaction> transactions = GenerationUtil.createQuasiSteadyStateTransactions(transactionsToSpend, blockHeight);
            _writeTransactionGenerationOrder(null, transactions, _scenarioDirectory, blockHeight);
            return transactions;
        }
        catch (final Exception exception) {
            Logger.warn(exception);
            return null;
        }
    }
}