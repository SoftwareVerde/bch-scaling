package com.softwareverde.bitcoin.scaling.generate;

import com.softwareverde.bitcoin.address.Address;
import com.softwareverde.bitcoin.address.AddressInflater;
import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.scaling.DiskUtil;
import com.softwareverde.bitcoin.scaling.Main;
import com.softwareverde.bitcoin.server.database.BatchRunner;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.wallet.PaymentAmount;
import com.softwareverde.bitcoin.wallet.Wallet;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.immutable.ImmutableList;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.cryptography.secp256k1.key.PrivateKey;
import com.softwareverde.logging.Logger;
import com.softwareverde.util.timer.MultiTimer;
import com.softwareverde.util.timer.NanoTimer;

import java.io.File;
import java.util.concurrent.ConcurrentLinkedQueue;

public class FanOutBlockTransactionGenerator extends TransactionGenerator {
    public FanOutBlockTransactionGenerator(final PrivateKeyGenerator privateKeyGenerator, final File scenarioDirectory, final int coinbaseMaturityBlockCount) {
        super(privateKeyGenerator, scenarioDirectory, coinbaseMaturityBlockCount);
    }

    @Override
    public List<Transaction> getTransactions(final Long blockHeight, final List<BlockHeader> existingBlockHeaders, final List<BlockHeader> createdBlocks) {
        final long coinbaseToSpendBlockHeight = (blockHeight - _coinbaseMaturityBlockCount); // preFanOutBlock...
        final BlockHeader blockHeader = existingBlockHeaders.get((int) coinbaseToSpendBlockHeight);
        final Block block = DiskUtil.loadBlock(blockHeader.getHash(), _scenarioDirectory);

        final Transaction transactionToSplit = block.getCoinbaseTransaction();
        final PrivateKey coinbasePrivateKey = _privateKeyGenerator.getCoinbasePrivateKey(coinbaseToSpendBlockHeight);

        // Spend the coinbase
        final Transaction transaction;
        final PrivateKey transactionPrivateKey;
        {
            final Wallet wallet = new Wallet();
            wallet.addPrivateKey(coinbasePrivateKey);
            wallet.addTransaction(transactionToSplit);

            final Address address = wallet.getReceivingAddress();
            final Long totalOutputValue = transactionToSplit.getTotalOutputValue();

            final int outputCount = 4;
            final Long outputValue = (totalOutputValue / outputCount) - 250L;
            final MutableList<PaymentAmount> paymentAmounts = new MutableList<>(outputCount);
            for (int i = 0; i < outputCount; ++i) {
                paymentAmounts.add(new PaymentAmount(address, outputValue));
            }

            transaction = wallet.createTransaction(paymentAmounts, address);
            if (transaction == null) {
                Logger.warn("Unable to create transaction.");
            }

            transactionPrivateKey = coinbasePrivateKey;
        }

        try {
            final MutableList<Transaction> transactions = _createFanOutTransactions(transaction, transactionPrivateKey, blockHeight);
            _writeTransactionGenerationOrder(transaction, transactions, _scenarioDirectory, blockHeight);
            transactions.add(transaction);
            return transactions;
        }
        catch (final Exception exception) {
            Logger.warn(exception);
            return null;
        }
    }

    protected MutableList<Transaction> _createFanOutTransactions(final Transaction rootTransactionToSpend, final PrivateKey privateKey, final Long blockHeight) throws Exception {
        final int transactionCount = 256000;
        final int outputsPerTransactionCount = 25; // TxSize = 158 + (34 * OutputCount) ~= 1024
        final long minOutputAmount = 546;

        final AddressInflater addressInflater = new AddressInflater();

        final NanoTimer nanoTimer = new NanoTimer();

        nanoTimer.start();
        final ConcurrentLinkedQueue<Transaction> transactions = new ConcurrentLinkedQueue<>();

        final BatchRunner<Integer> batchRunner = new BatchRunner<>(1, true);
        batchRunner.run(new ImmutableList<>(0, 1, 2, 3), new BatchRunner.Batch<Integer>() {
            @Override
            public void run(final List<Integer> batchItems) throws Exception {
                Transaction transactionToSpend = rootTransactionToSpend;
                final MutableList<Integer> possibleOutputsList = new MutableList<>();
                {
                    final Integer index = batchItems.get(0);
                    possibleOutputsList.add(index);
                }

                final MultiTimer multiTimer = new MultiTimer();
                multiTimer.start();

                final int batchTransactionCount = (transactionCount / 4);
                for (int i = 0; i < batchTransactionCount; ++i) {
                    multiTimer.mark("batchStart");
                    final MutableList<PaymentAmount> paymentAmounts = new MutableList<>();
                    for (int j = 0; j < outputsPerTransactionCount; ++j) {
                        final Long amount = minOutputAmount + ((long) (Math.random() * 4268L / 4));
                        final PrivateKey recipientPrivateKey = Main.derivePrivateKey(blockHeight, amount);
                        final Address recipientAddress = addressInflater.fromPrivateKey(recipientPrivateKey, true);

                        paymentAmounts.add(new PaymentAmount(recipientAddress, amount));
                    }
                    multiTimer.mark("paymentAmounts");

                    final Wallet wallet = new Wallet();
                    wallet.setSatoshisPerByteFee(1D);

                    wallet.addPrivateKey(privateKey);
                    if (possibleOutputsList.isEmpty()) {
                        wallet.addTransaction(transactionToSpend);
                    }
                    else {
                        wallet.addTransaction(transactionToSpend, possibleOutputsList);
                    }

                    multiTimer.mark("walletInit");

                    final Transaction transaction = wallet.createTransaction(paymentAmounts, wallet.getReceivingAddress());
                    if (transaction == null) {
                        Logger.debug("Unable to create transaction. (Insufficient funds?)");
                        break;
                    }

                    multiTimer.mark("createTransaction");

                    transactions.add(transaction);
                    multiTimer.mark("addTransaction");

                    nanoTimer.stop();

                    if (i % 1024 == 0) {
                        final double msElapsed = nanoTimer.getMillisecondsElapsed();
                        final int txPerSec = (int) (i * 1000L / msElapsed);
                        Logger.debug(i + " of " + batchTransactionCount + " transactions. (" + txPerSec + " tx/sec) " + multiTimer);
                    }

                    transactionToSpend = transaction;
                    if (i == 0) {
                        possibleOutputsList.clear();
                    }
                }
            }
        });

        return new MutableList<>(transactions);
    }
}