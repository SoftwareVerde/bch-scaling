package com.softwareverde.bitcoin.scaling.generate;

import com.softwareverde.bitcoin.address.Address;
import com.softwareverde.bitcoin.address.AddressInflater;
import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.header.BlockHeaderInflater;
import com.softwareverde.bitcoin.scaling.DiskUtil;
import com.softwareverde.bitcoin.scaling.Main;
import com.softwareverde.bitcoin.server.database.BatchRunner;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.transaction.TransactionDeflater;
import com.softwareverde.bitcoin.transaction.output.TransactionOutput;
import com.softwareverde.bitcoin.wallet.PaymentAmount;
import com.softwareverde.bitcoin.wallet.Wallet;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.cryptography.secp256k1.key.PrivateKey;
import com.softwareverde.logging.Logger;
import com.softwareverde.util.ByteUtil;
import com.softwareverde.util.timer.NanoTimer;

import java.io.File;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

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
            final List<Transaction> transactions = _createFanInTransactions(transactionsToSpend, blockHeight);
            _writeTransactionGenerationOrder(null, transactions, _scenarioDirectory, blockHeight);
            return transactions;
        }
        catch (final Exception exception) {
            Logger.warn(exception);
            return null;
        }
    }

    protected MutableList<Transaction> _createFanInTransactions(final List<TransactionWithBlockHeight> transactionsToSpend, final Long blockHeight) throws Exception {
        final Long maxBlockSize = 256L * ByteUtil.Unit.Si.MEGABYTES;

        final AddressInflater addressInflater = new AddressInflater();

        final NanoTimer nanoTimer = new NanoTimer();

        nanoTimer.start();
        final ConcurrentLinkedQueue<Transaction> transactions = new ConcurrentLinkedQueue<>();

        final TransactionDeflater transactionDeflater = new TransactionDeflater();
        final AtomicLong blockSize = new AtomicLong(BlockHeaderInflater.BLOCK_HEADER_BYTE_COUNT + 8);

        final long minOutputAmount = 546;
        final int batchSize = 32;
        final BatchRunner<TransactionWithBlockHeight> batchRunner = new BatchRunner<>(batchSize, true, 4);
        batchRunner.run(transactionsToSpend, new BatchRunner.Batch<>() {
            @Override
            public void run(final List<TransactionWithBlockHeight> batchItems) throws Exception {
                if (blockSize.get() >= maxBlockSize) { return; }

                final NanoTimer nanoTimer = new NanoTimer();
                nanoTimer.start();

                final Wallet wallet = new Wallet();
                wallet.setSatoshisPerByteFee(1D);

                int outputsToSpendCount = 0;
                for (final TransactionWithBlockHeight transactionWithBlockHeight : batchItems) {
                    for (final TransactionOutput transactionOutput : transactionWithBlockHeight.transaction.getTransactionOutputs()) {
                        final Long amount = transactionOutput.getAmount();
                        final PrivateKey privateKey = Main.derivePrivateKey(transactionWithBlockHeight.blockHeight, amount);
                        wallet.addPrivateKey(privateKey);

                        outputsToSpendCount += 1;
                    }
                    wallet.addTransaction(transactionWithBlockHeight.transaction);
                }

                final Long totalAmount = wallet.getBalance();
                if (totalAmount < 1L) {
                    Logger.debug("Zero wallet balance; invalid private key?");
                }

                final long maxFee = 100000L;
                Long fee = wallet.calculateFees(1, outputsToSpendCount);
                Transaction transaction = null;
                while (transaction == null && fee <= maxFee) {
                    final Long amount = (totalAmount - fee);
                    if (amount < minOutputAmount) { break; }

                    final PrivateKey privateKey = Main.derivePrivateKey(blockHeight, amount);
                    final Address address = addressInflater.fromPrivateKey(privateKey, true);

                    final MutableList<PaymentAmount> paymentAmounts = new MutableList<>();
                    paymentAmounts.add(new PaymentAmount(address, amount));

                    transaction = wallet.createTransaction(paymentAmounts, address);
                    fee += 500L;
                }
                if (transaction == null) {
                    Logger.debug("Unable to create transaction.");
                    return;
                }

                final Integer byteCount = transactionDeflater.getByteCount(transaction);
                final long newBlockSize = blockSize.addAndGet(byteCount);
                if (newBlockSize >= maxBlockSize) { return; }

                transactions.add(transaction);

                nanoTimer.stop();
                // Logger.debug("Spent " + batchItems.getCount() + " transactions in " + nanoTimer.getMillisecondsElapsed() + "ms.");
            }
        });

        return new MutableList<>(transactions);
    }
}