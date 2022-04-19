package com.softwareverde.bitcoin.scaling;

import com.softwareverde.bitcoin.address.Address;
import com.softwareverde.bitcoin.address.AddressInflater;
import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.server.database.BatchRunner;
import com.softwareverde.bitcoin.transaction.MutableTransaction;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.transaction.TransactionDeflater;
import com.softwareverde.bitcoin.transaction.input.TransactionInput;
import com.softwareverde.bitcoin.transaction.locktime.LockTime;
import com.softwareverde.bitcoin.transaction.output.MutableTransactionOutput;
import com.softwareverde.bitcoin.transaction.output.TransactionOutput;
import com.softwareverde.bitcoin.wallet.PaymentAmount;
import com.softwareverde.bitcoin.wallet.Wallet;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.immutable.ImmutableList;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.cryptography.secp256k1.key.PrivateKey;
import com.softwareverde.logging.LogLevel;
import com.softwareverde.logging.Logger;
import com.softwareverde.util.timer.MultiTimer;
import com.softwareverde.util.timer.NanoTimer;

import java.util.concurrent.ConcurrentLinkedQueue;

public class MainTest {
    protected static List<Transaction> _createTransactions(final Long blockHeight) throws Exception {
        final int transactionCount = 256000;
        final int outputsPerTransactionCount = 25; // TxSize = 158 + (34 * OutputCount) ~= 1024

        final AddressInflater addressInflater = new AddressInflater();
        final PrivateKey privateKey = PrivateKey.createNewKey();

        final Transaction rootTransactionToSpend;
        {
            final MutableTransaction transaction = new MutableTransaction();
            transaction.setVersion(Transaction.VERSION);
            transaction.setLockTime(LockTime.MAX_TIMESTAMP);

            final TransactionInput transactionInput = TransactionInput.createCoinbaseTransactionInput(0L, "");
            transaction.addTransactionInput(transactionInput);

            final Address address = addressInflater.fromPrivateKey(privateKey, true);
            {
                final MutableTransactionOutput transactionOutput = new MutableTransactionOutput(TransactionOutput.createPayToAddressTransactionOutput(address, (long) (12.5D * Transaction.SATOSHIS_PER_BITCOIN)));
                transactionOutput.setIndex(0);
                transaction.addTransactionOutput(transactionOutput);
            }

            {
                final MutableTransactionOutput transactionOutput = new MutableTransactionOutput(TransactionOutput.createPayToAddressTransactionOutput(address, (long) (12.5D * Transaction.SATOSHIS_PER_BITCOIN)));
                transactionOutput.setIndex(1);
                transaction.addTransactionOutput(transactionOutput);
            }

            {
                final MutableTransactionOutput transactionOutput = new MutableTransactionOutput(TransactionOutput.createPayToAddressTransactionOutput(address, (long) (12.5D * Transaction.SATOSHIS_PER_BITCOIN)));
                transactionOutput.setIndex(2);
                transaction.addTransactionOutput(transactionOutput);
            }

            {
                final MutableTransactionOutput transactionOutput = new MutableTransactionOutput(TransactionOutput.createPayToAddressTransactionOutput(address, (long) (12.5D * Transaction.SATOSHIS_PER_BITCOIN)));
                transactionOutput.setIndex(3);
                transaction.addTransactionOutput(transactionOutput);
            }

            rootTransactionToSpend = transaction.asConst();
        }

        final ConcurrentLinkedQueue<Transaction> transactions = new ConcurrentLinkedQueue<>();

        final BatchRunner<Integer> batchRunner = new BatchRunner<>(1, true);
        batchRunner.run(new ImmutableList<>(0, 1, 2, 3), new BatchRunner.Batch<>() {
            @Override
            public void run(final List<Integer> batchItems) throws Exception {
                final NanoTimer nanoTimer = new NanoTimer();
                nanoTimer.start();

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
                        final Long amount = 500L + ((long) (Math.random() * 4268L / 4));
                        final PrivateKey recipientPrivateKey = Main.derivePrivateKey(blockHeight, amount);
                        final Address recipientAddress = addressInflater.fromPrivateKey(recipientPrivateKey, true);

                        paymentAmounts.add(new PaymentAmount(recipientAddress, amount));
                    }
                    multiTimer.mark("paymentAmounts");

                    final Wallet wallet = new Wallet();
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
                        return;
                    }

                    multiTimer.mark("createTransaction");

                    transactions.add(transaction);
                    multiTimer.mark("addTransaction");

                    if (i % 1024 == 0) {
                        nanoTimer.stop();
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

        return new ImmutableList<>(transactions);
    }

    public static void foo() throws Exception {
        Logger.setLogLevel(LogLevel.ON);
        Logger.setLogLevel("com.softwareverde.bitcoin.wallet.Wallet", LogLevel.WARN);

        final Main main = new Main() {
            @Override
            protected void _sendBlock(final Block block) {
                // Nothing.
            }
        };

        final List<Transaction> transactions = _createTransactions(245L);
        for (int i = 0; i < 12; ++i) {
            System.out.println((new TransactionDeflater()).toBytes(transactions.get(i)));
        }
    }
}
