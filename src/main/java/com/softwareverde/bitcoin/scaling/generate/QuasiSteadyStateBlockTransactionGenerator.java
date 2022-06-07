package com.softwareverde.bitcoin.scaling.generate;

import com.softwareverde.bitcoin.address.Address;
import com.softwareverde.bitcoin.address.AddressInflater;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.header.BlockHeaderInflater;
import com.softwareverde.bitcoin.scaling.Main;
import com.softwareverde.bitcoin.server.database.BatchRunner;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.transaction.TransactionDeflater;
import com.softwareverde.bitcoin.transaction.output.TransactionOutput;
import com.softwareverde.bitcoin.wallet.PaymentAmount;
import com.softwareverde.bitcoin.wallet.Wallet;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.cryptography.secp256k1.key.PrivateKey;
import com.softwareverde.logging.Logger;
import com.softwareverde.util.ByteUtil;
import com.softwareverde.util.timer.NanoTimer;

import java.io.File;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public abstract class QuasiSteadyStateBlockTransactionGenerator extends TransactionGenerator {
    public QuasiSteadyStateBlockTransactionGenerator(PrivateKeyGenerator privateKeyGenerator, File scenarioDirectory, int coinbaseMaturityBlockCount) {
        super(privateKeyGenerator, scenarioDirectory, coinbaseMaturityBlockCount);
    }

    protected abstract List<TransactionWithBlockHeight> _collectTransactionsToSpend(Long blockHeight, List<BlockHeader> existingBlockHeaders, List<BlockHeader> newBlockHeaders);

    @Override
    public List<Transaction> getTransactions(Long blockHeight, List<BlockHeader> existingBlockHeaders, List<BlockHeader> newBlockHeaders) {
        try {
            final List<TransactionWithBlockHeight> transactionsToSpend = _collectTransactionsToSpend(blockHeight, existingBlockHeaders, newBlockHeaders);
            final List<Transaction> transactions = _createQuasiSteadyStateTransactions(transactionsToSpend, blockHeight);
            _writeTransactionGenerationOrder(null, transactions, _scenarioDirectory, blockHeight);
            return transactions;
        }
        catch (final Exception exception) {
            Logger.warn(exception);
            return null;
        }
    }

    protected MutableList<Transaction> _createQuasiSteadyStateTransactions(final List<TransactionWithBlockHeight> transactionsToSpend, final Long blockHeight) throws Exception {
        final Long maxBlockSize = 256L * ByteUtil.Unit.Si.MEGABYTES;

        final AddressInflater addressInflater = new AddressInflater();

        final NanoTimer nanoTimer = new NanoTimer();

        nanoTimer.start();
        final ConcurrentLinkedQueue<Transaction> transactions = new ConcurrentLinkedQueue<>();

        final TransactionDeflater transactionDeflater = new TransactionDeflater();
        final AtomicLong blockSize = new AtomicLong(BlockHeaderInflater.BLOCK_HEADER_BYTE_COUNT + 8L);

        final int batchSize = 128;
        final BatchRunner<TransactionWithBlockHeight> batchRunner = new BatchRunner<>(batchSize, true, 4);
        batchRunner.run(transactionsToSpend, new BatchRunner.Batch<>() {
            @Override
            public void run(final List<TransactionWithBlockHeight> batchItems) throws Exception {
                if (blockSize.get() >= maxBlockSize) { return; }

                final NanoTimer nanoTimer = new NanoTimer();
                nanoTimer.start();

                long fee = 1500;
                final long maxFee = 100000L;
                final long minOutputAmount = 546;

                int outputsToSpendCount = 0;
                for (final TransactionWithBlockHeight transactionWithBlockHeight : batchItems) {
                    final Wallet wallet = new Wallet();
                    wallet.setSatoshisPerByteFee(1D);

                    for (final TransactionOutput transactionOutput : transactionWithBlockHeight.transaction.getTransactionOutputs()) {
                        final Long amount = transactionOutput.getAmount();
                        final PrivateKey privateKey = Main.derivePrivateKey(transactionWithBlockHeight.blockHeight, amount);
                        wallet.addPrivateKey(privateKey);

                        outputsToSpendCount += 1;
                    }
                    wallet.addTransaction(transactionWithBlockHeight.transaction);

                    // long fee = wallet.calculateFees(2, outputsToSpendCount);

                    Transaction transaction = null;
                    while (transaction == null && fee <= maxFee) {
                        final Long amount = wallet.getBalance();
                        if (amount < 1L) {
                            Logger.debug("Zero wallet balance; invalid private key?");
                        }
                        final Long amount0 = (amount / 2L);
                        final Long amount1 = (amount - amount0 - fee);

                        if (amount1 < minOutputAmount) { break; }

                        final Address changeAddress;
                        final MutableList<PaymentAmount> paymentAmounts = new MutableList<>();
                        {
                            final PrivateKey privateKey = Main.derivePrivateKey(blockHeight, amount0);
                            final Address address = addressInflater.fromPrivateKey(privateKey, true);

                            paymentAmounts.add(new PaymentAmount(address, amount0));
                        }
                        {
                            final PrivateKey privateKey = Main.derivePrivateKey(blockHeight, amount1);
                            final Address address = addressInflater.fromPrivateKey(privateKey, true);

                            paymentAmounts.add(new PaymentAmount(address, amount1));
                            changeAddress = address;
                        }

                        transaction = wallet.createTransaction(paymentAmounts, changeAddress);
                        if (transaction == null) {
                            fee += 500L;
                            // Logger.debug("Setting Fee: " + fee + "; amount=" + amount + " amount0=" + amount0 + " amount1=" + amount1);
                        }
                    }
                    if (transaction == null) {
                        Logger.debug("Unable to create transaction.");
                        break;
                    }

                    final Integer byteCount = transactionDeflater.getByteCount(transaction);
                    final long newBlockSize = blockSize.addAndGet(byteCount);
                    if (newBlockSize >= maxBlockSize) {
                        Logger.debug("Max block size reached: " + newBlockSize + " of " + maxBlockSize);
                        return;
                    }

                    transactions.add(transaction);

                    nanoTimer.stop();
                }
            }
        });

        final MutableList<Transaction> createdTransactions = new MutableList<>(transactions);
        Logger.debug("Created " + createdTransactions.getCount() + " transactions.");
        return createdTransactions;
    }
}