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

public class QuasiSteadyStateRound2BlockTransactionGenerator extends QuasiSteadyStateBlockTransactionGenerator {
    protected long _startingBlockHeight;
    protected long _firstFanInBlockHeight;
    protected long _firstFanOutBlockHeight;

    public QuasiSteadyStateRound2BlockTransactionGenerator(PrivateKeyGenerator privateKeyGenerator, File scenarioDirectory, int coinbaseMaturityBlockCount, long startingBlockHeight, long firstFanInBlockHeight, long firstFanOutBlockHeight) {
        super(privateKeyGenerator, scenarioDirectory, coinbaseMaturityBlockCount);
        _startingBlockHeight = startingBlockHeight;
        _firstFanInBlockHeight = firstFanInBlockHeight;
        _firstFanOutBlockHeight = firstFanOutBlockHeight;
    }

    @Override
    protected List<TransactionWithBlockHeight> _collectTransactionsToSpend(final Long blockHeight, final List<BlockHeader> existingBlockHeaders, final List<BlockHeader> createdBlocks) {
        final MutableList<TransactionWithBlockHeight> transactionsToSpend = new MutableList<>();
        {
            final int steadyStateBlockIndex = (int) (blockHeight - _startingBlockHeight); // the Nth of 5 steady state blocks...

            // Spend the respective fan-in block...
            {
                final long blockHeightToSpend = (_firstFanInBlockHeight + steadyStateBlockIndex);
                final Sha256Hash blockHash = Main.getBlockHash(blockHeightToSpend, existingBlockHeaders, createdBlocks);
                final Block blockToSpend = DiskUtil.loadBlock(blockHash, _scenarioDirectory);
                final List<Transaction> transactions = blockToSpend.getTransactions();
                for (int i = 1; i < transactions.getCount(); ++i) {
                    final Transaction transaction = transactions.get(i);
                    transactionsToSpend.add(new TransactionWithBlockHeight(transaction, blockHeightToSpend));
                }
            }

            // And spend only the second half of each 10 fan-out blocks, over 5 steady-state blocks, means each steady state block spends 1/5 of 1/2 of each fan-out block.
            for (int i = 0; i < 10; ++i) {
                final long blockHeightToSpend = (_firstFanOutBlockHeight + steadyStateBlockIndex + i);
                final Sha256Hash blockHash = Main.getBlockHash(blockHeightToSpend, existingBlockHeaders, createdBlocks);
                final Block blockToSpend = DiskUtil.loadBlock(blockHash, _scenarioDirectory);

                final int transactionCount = blockToSpend.getTransactionCount();
                final int transactionCountToSpend = (transactionCount / 5);
                final List<Transaction> transactions = blockToSpend.getTransactions();
                final int startIndex = (1 + (transactionCountToSpend * steadyStateBlockIndex)) + (transactionCount / 2);
                for (int j = startIndex; j < transactionCountToSpend; ++j) {
                    if (j >= transactionCount) { break; }

                    final Transaction transaction = transactions.get(j);
                    transactionsToSpend.add(new TransactionWithBlockHeight(transaction, blockHeightToSpend));
                }
            }
        }
        return transactionsToSpend;
    }
}