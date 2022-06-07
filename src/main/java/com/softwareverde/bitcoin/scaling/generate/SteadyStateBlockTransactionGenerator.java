package com.softwareverde.bitcoin.scaling.generate;

import com.softwareverde.bitcoin.address.Address;
import com.softwareverde.bitcoin.address.AddressInflater;
import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.scaling.DiskUtil;
import com.softwareverde.bitcoin.scaling.Main;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.util.ByteUtil;
import com.softwareverde.bitcoin.wallet.PaymentAmount;
import com.softwareverde.bitcoin.wallet.SlimWallet;
import com.softwareverde.bitcoin.wallet.SpendableTransactionOutput;
import com.softwareverde.bitcoin.wallet.Wallet;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.cryptography.secp256k1.key.PrivateKey;
import com.softwareverde.logging.LogLevel;
import com.softwareverde.logging.Logger;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Comparator;

public class SteadyStateBlockTransactionGenerator extends TransactionGenerator {
    public static final Comparator<SpendableTransactionOutput> testUtxoComparator = new Comparator<SpendableTransactionOutput>() {
        @Override
        public int compare(final SpendableTransactionOutput utxo0, final SpendableTransactionOutput utxo1) {
            final Sha256Hash transactionHash0 = utxo0.getTransactionHash();
            final long value0 = ByteUtil.byteToLong(transactionHash0.getByte(Sha256Hash.BYTE_COUNT - 1)) + (utxo0.getIndex() * utxo0.getAmount());

            final Sha256Hash transactionHash1 = utxo1.getTransactionHash();
            final long value1 = ByteUtil.byteToLong(transactionHash1.getByte(Sha256Hash.BYTE_COUNT - 1)) + (utxo1.getIndex() * utxo1.getAmount());

            return Long.compare(value0, value1);
        }
    };

    protected final long _startingBlockHeight;
    protected final MutableList<SpendableTransactionOutput> _availableUtxos;

    public SteadyStateBlockTransactionGenerator(PrivateKeyGenerator privateKeyGenerator, File scenarioDirectory, int coinbaseMaturityBlockCount, final MutableList<SpendableTransactionOutput> availableUtxos, final long startingBlockHeight) {
        super(privateKeyGenerator, scenarioDirectory, coinbaseMaturityBlockCount);
        _availableUtxos = availableUtxos;
        _startingBlockHeight = startingBlockHeight;
    }

    @Override
    public List<Transaction> getTransactions(final Long blockHeight, final List<BlockHeader> existingBlockHeaders, final List<BlockHeader> createdBlocks) {
        final int blockOffset = (int) (blockHeight - _startingBlockHeight);
        final long spendBlockHeight = (156L + blockOffset);
        Logger.debug("Spending Block #" + spendBlockHeight + "'s coinbase.");
        final Transaction additionalTransactionFromPreviousCoinbase = _splitCoinbaseTransaction(blockHeight, spendBlockHeight, existingBlockHeaders, _scenarioDirectory, _privateKeyGenerator);
        { // Add the coinbase UTXOs to the pool.
            _availableUtxos.addAll(SlimWallet.getTransactionOutputs(additionalTransactionFromPreviousCoinbase, blockHeight, false));

            // Arbitrarily re-order/mix the UTXOs...
            _availableUtxos.sort(testUtxoComparator);
        }

        Logger.debug("availableUtxos.count=" + _availableUtxos.getCount());

        try {
            final ArrayDeque<SpendableTransactionOutput> utxoDeque = new ArrayDeque<>();
            for (final SpendableTransactionOutput utxo : _availableUtxos) {
                utxoDeque.add(utxo);
            }
            final List<Transaction> generatedTransactions = GenerationUtil.createCashTransactions(additionalTransactionFromPreviousCoinbase, utxoDeque, blockHeight);

            _availableUtxos.clear();
            _availableUtxos.addAll(utxoDeque);

            _writeTransactionGenerationOrder(additionalTransactionFromPreviousCoinbase, generatedTransactions, _scenarioDirectory, blockHeight);

            final MutableList<Transaction> transactions = new MutableList<>(generatedTransactions.getCount() + 1);
            transactions.add(additionalTransactionFromPreviousCoinbase);
            transactions.addAll(generatedTransactions);
            return transactions;
        }
        catch (final Exception exception) {
            Logger.warn(exception);
            return null;
        }
    }

    protected Transaction _splitCoinbaseTransaction(final Long blockHeight, final Long coinbaseToSpendBlockHeight, final List<BlockHeader> blockHeaders, final File _scenarioDirectory, final PrivateKeyGenerator privateKeyGenerator) {
        final BlockHeader blockHeader = blockHeaders.get(coinbaseToSpendBlockHeight.intValue());
        Logger.debug("Splitting Coinbase: " + blockHeader.getHash());
        final Block block = DiskUtil.loadBlock(blockHeader.getHash(), _scenarioDirectory);

        final Transaction transactionToSplit = block.getCoinbaseTransaction();
        final PrivateKey coinbasePrivateKey = privateKeyGenerator.getCoinbasePrivateKey(coinbaseToSpendBlockHeight);

        final Wallet wallet = new Wallet();
        wallet.addPrivateKey(coinbasePrivateKey);
        wallet.addTransaction(transactionToSplit);

        final Address changeAddress = wallet.getReceivingAddress();
        final Long totalOutputValue = transactionToSplit.getTotalOutputValue();

        final int outputCount = 200;
        final Long outputValue = (totalOutputValue / outputCount) - 15000L;

        final Address address;
        {
            final PrivateKey privateKey = Main.derivePrivateKey(blockHeight, outputValue);
            final AddressInflater addressInflater = new AddressInflater();
            address = addressInflater.fromPrivateKey(privateKey, true);
        }

        final MutableList<PaymentAmount> paymentAmounts = new MutableList<>(outputCount);
        for (int i = 0; i < outputCount; ++i) {
            paymentAmounts.add(new PaymentAmount(address, outputValue));
        }

        final Transaction transaction = wallet.createTransaction(paymentAmounts, changeAddress);
        if (transaction == null) {
            Logger.warn("Unable to create transaction.");

            { // Debug.
                Logger.setLogLevel("com.softwareverde.bitcoin.wallet.Wallet", LogLevel.ON);
                wallet.createTransaction(paymentAmounts, changeAddress);
            }
        }

        return transaction;
    }
}