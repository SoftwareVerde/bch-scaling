package com.softwareverde.bitcoin.scaling.generate;

import com.softwareverde.bitcoin.address.Address;
import com.softwareverde.bitcoin.address.AddressInflater;
import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockDeflater;
import com.softwareverde.bitcoin.block.BlockInflater;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.header.BlockHeaderInflater;
import com.softwareverde.bitcoin.block.header.difficulty.Difficulty;
import com.softwareverde.bitcoin.block.validator.difficulty.DifficultyCalculator;
import com.softwareverde.bitcoin.context.DifficultyCalculatorContext;
import com.softwareverde.bitcoin.rpc.BlockTemplate;
import com.softwareverde.bitcoin.rpc.MutableBlockTemplate;
import com.softwareverde.bitcoin.scaling.Main;
import com.softwareverde.bitcoin.scaling.PrivateTestNetDifficultyCalculatorContext;
import com.softwareverde.bitcoin.scaling.TestUtxo;
import com.softwareverde.bitcoin.scaling.TransactionGenerator;
import com.softwareverde.bitcoin.scaling.TransactionWithBlockHeight;
import com.softwareverde.bitcoin.server.database.BatchRunner;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.transaction.TransactionDeflater;
import com.softwareverde.bitcoin.transaction.coinbase.CoinbaseTransaction;
import com.softwareverde.bitcoin.transaction.input.TransactionInput;
import com.softwareverde.bitcoin.transaction.output.TransactionOutput;
import com.softwareverde.bitcoin.transaction.output.identifier.TransactionOutputIdentifier;
import com.softwareverde.bitcoin.wallet.PaymentAmount;
import com.softwareverde.bitcoin.wallet.Wallet;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.immutable.ImmutableList;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.cryptography.secp256k1.key.PrivateKey;
import com.softwareverde.logging.Logger;
import com.softwareverde.util.ByteUtil;
import com.softwareverde.util.IoUtil;
import com.softwareverde.util.Util;
import com.softwareverde.util.timer.MultiTimer;
import com.softwareverde.util.timer.NanoTimer;
import com.softwareverde.util.type.time.SystemTime;

import java.io.File;
import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class GenerationUtil {
    protected GenerationUtil() { }

    public static BlockTemplate getBlockTemplate(final File blocksDirectory, final Sha256Hash previousBlockHash, final Long nextBlockHeight, final TransactionGenerator transactionGenerator, final PrivateTestNetDifficultyCalculatorContext difficultyCalculatorContext, final List<BlockHeader> createdBlocks) {
        final File templatesDirectory = new File(blocksDirectory, "templates");
        if (! templatesDirectory.exists()) {
            templatesDirectory.mkdirs();
        }

        final File templateFile = new File(templatesDirectory, nextBlockHeight.toString());
        if (templateFile.exists()) {
            Logger.debug("Using Template: " + templateFile.getPath());
            final Block block;
            {
                final BlockInflater blockInflater = new BlockInflater();
                final ByteArray bytes = ByteArray.wrap(IoUtil.getFileContents(templateFile));
                block = blockInflater.fromBytes(bytes);
            }

            final MutableBlockTemplate blockTemplate = new MutableBlockTemplate();
            blockTemplate.setBlockVersion(Block.VERSION);
            blockTemplate.setDifficulty(block.getDifficulty());
            blockTemplate.setPreviousBlockHash(previousBlockHash);
            blockTemplate.setBlockHeight(nextBlockHeight);

            final CoinbaseTransaction coinbaseTransaction = block.getCoinbaseTransaction();
            blockTemplate.setCoinbaseAmount(coinbaseTransaction.getTotalOutputValue());

            final Long blockTime = block.getTimestamp();
            blockTemplate.setMinimumBlockTime(blockTime);
            blockTemplate.setCurrentTime(blockTime);

            boolean isCoinbase = true;
            final List<Transaction> transactions = block.getTransactions();
            for (final Transaction transaction : transactions) {
                if (isCoinbase) {
                    isCoinbase = false;
                    continue;
                }

                blockTemplate.addTransaction(transaction, 0L, 0);
            }

            return blockTemplate;
        }

        final List<Transaction> transactions = ((transactionGenerator != null) ? transactionGenerator.getTransactions(nextBlockHeight, createdBlocks) : new MutableList<>(0));
        final BlockTemplate blockTemplate = GenerationUtil.createBlockTemplate(previousBlockHash, nextBlockHeight, transactions, difficultyCalculatorContext);

        try {
            final Block block = blockTemplate.toBlock();

            final BlockDeflater blockDeflater = new BlockDeflater();
            final ByteArray blockBytes = blockDeflater.toBytes(block);
            IoUtil.putFileContents(templateFile, blockBytes);
        }
        catch (final Exception exception) {
            Logger.debug(exception);
        }

        return blockTemplate;
    }

    public static BlockTemplate createBlockTemplate(final Sha256Hash previousBlockHash, final Long blockHeight, final List<Transaction> transactions, final DifficultyCalculatorContext difficultyCalculatorContext) {
        final SystemTime systemTime = new SystemTime();

        final DifficultyCalculator difficultyCalculator = new DifficultyCalculator(difficultyCalculatorContext);

        final Difficulty difficulty = difficultyCalculator.calculateRequiredDifficulty(blockHeight);

        final Long coinbaseReward = BlockHeader.calculateBlockReward(blockHeight);

        final MutableBlockTemplate blockTemplate = new MutableBlockTemplate();
        blockTemplate.setBlockVersion(Block.VERSION);
        blockTemplate.setDifficulty(difficulty);
        blockTemplate.setPreviousBlockHash(previousBlockHash);
        blockTemplate.setBlockHeight(blockHeight);
        blockTemplate.setCoinbaseAmount(coinbaseReward);

        final Long blockTime = systemTime.getCurrentTimeInSeconds();
        blockTemplate.setMinimumBlockTime(blockTime);
        blockTemplate.setCurrentTime(blockTime);

        for (final Transaction transaction : transactions) {
            blockTemplate.addTransaction(transaction, 0L, 0);
        }

        return blockTemplate;
    }

    public static MutableList<Transaction> createFanOutTransactions(final Transaction rootTransactionToSpend, final PrivateKey privateKey, final Long blockHeight) throws Exception {
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

    public static MutableList<Transaction> createQuasiSteadyStateTransactions(final List<TransactionWithBlockHeight> transactionsToSpend, final Long blockHeight) throws Exception {
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

    public static class DebugWallet extends Wallet {
        public void debugWalletState() {
            _debugWalletState();
        }
    }

    public static MutableList<Transaction> createCashTransactions(final MutableList<TestUtxo> availableUtxos, final Long blockHeight) throws Exception {
        final long maxBlockSize = 256L * ByteUtil.Unit.Si.MEGABYTES;

        final AddressInflater addressInflater = new AddressInflater();

        final NanoTimer nanoTimer = new NanoTimer();

        nanoTimer.start();
        final ConcurrentLinkedQueue<Transaction> transactions = new ConcurrentLinkedQueue<>();

        final TransactionDeflater transactionDeflater = new TransactionDeflater();
        long blockSize = (BlockHeaderInflater.BLOCK_HEADER_BYTE_COUNT + 8L);

        // final long defaultMinBalance = 100000L;
        // long minBalance = defaultMinBalance;
        final long minBalance = 100000L;

        long availableBalance = 0L;
        for (final TestUtxo testUtxo : availableUtxos) {
            availableBalance += testUtxo.getAmount();
        }
        Logger.debug(availableUtxos.getCount() + " UTXOs available, with " + availableBalance + " satoshis total available.");

        int unspendableCount = 0;
        while (blockSize < maxBlockSize) {
            if (availableUtxos.isEmpty()) { break; }

            final long minOutputAmount = 546;

            final DebugWallet wallet = new DebugWallet();
            wallet.setSatoshisPerByteFee(1D);

            final MutableList<TransactionOutputIdentifier> outputIdentifiersToSpend = new MutableList<>();
            final HashSet<TransactionOutputIdentifier> outputsConsideredSpent = new HashSet<>();
            final MutableList<TestUtxo> testUtxos = new MutableList<>();
            int outputsToSpendCount = 0;
            long spendableWalletBalance = 0L;
            while ((spendableWalletBalance < minBalance) || (outputsToSpendCount < 2)) {
                if (availableUtxos.isEmpty()) { break; }

                final TestUtxo testUtxo = availableUtxos.remove(0);
                final PrivateKey privateKey = Main.derivePrivateKey(testUtxo.getBlockHeight(), testUtxo.getAmount());

                final Transaction transaction = testUtxo.getTransaction();

                final boolean isSpendable;
                {
                    final Integer outputIndex = testUtxo.getOutputIndex();
                    final Wallet isSpendableWallet = new Wallet();
                    isSpendableWallet.addPrivateKey(privateKey);
                    isSpendableWallet.addTransaction(transaction, new ImmutableList<>(outputIndex));
                    isSpendable = (isSpendableWallet.getBalance() > 0L);
                }
                if (! isSpendable) {
                    unspendableCount += 1;
                    continue;
                }

                wallet.addPrivateKey(privateKey);
                spendableWalletBalance += testUtxo.getAmount();

                final TransactionOutputIdentifier utxoOutputIdentifier = new TransactionOutputIdentifier(transaction.getHash(), testUtxo.getOutputIndex());
                outputIdentifiersToSpend.add(utxoOutputIdentifier);

                if (outputsConsideredSpent.contains(utxoOutputIdentifier)) {
                    outputsConsideredSpent.remove(utxoOutputIdentifier);
                }
                else {
                    final MutableList<TransactionOutputIdentifier> allTransactionOutputIdentifiers = TransactionOutputIdentifier.fromTransactionOutputs(transaction);
                    final int listIndex = allTransactionOutputIdentifiers.indexOf(utxoOutputIdentifier);
                    allTransactionOutputIdentifiers.remove(listIndex);
                    for (final TransactionOutputIdentifier transactionOutputIdentifier : allTransactionOutputIdentifiers) {
                        outputsConsideredSpent.add(transactionOutputIdentifier);
                    }

                    wallet.addTransaction(transaction);
                }

                outputsToSpendCount += 1;
                testUtxos.add(testUtxo);
            }
            for (final TransactionOutputIdentifier transactionOutputIdentifier : outputsConsideredSpent) {
                final Sha256Hash transactionHash = transactionOutputIdentifier.getTransactionHash();
                final Integer outputIndex = transactionOutputIdentifier.getOutputIndex();
                wallet.markTransactionOutputAsSpent(transactionHash, outputIndex);
            }

            if (wallet.getBalance() < minBalance) {
                Logger.debug("Unable to load min balance: " + wallet.getBalance());
                wallet.debugWalletState();
                continue;
            }

            // long fee = wallet.calculateFees(2, outputsToSpendCount);

            Transaction transaction = null;
            while (true) {
                final Long walletBalance = wallet.getBalance();
                final long amount = ((walletBalance / 2L) - 220L);
                if (amount < minOutputAmount) { break; } // Should never happen.

                final Address address;
                final MutableList<PaymentAmount> paymentAmounts = new MutableList<>(1);
                {
                    final PrivateKey privateKey = Main.derivePrivateKey(blockHeight, amount);
                    address = addressInflater.fromPrivateKey(privateKey, true);

                    paymentAmounts.add(new PaymentAmount(address, amount));
                }

                final Address tempChangeAddress;
                {
                    final PrivateKey privateKey = PrivateKey.fromHexString("0000000000000000000000000000000000000000000000000000000000000001");
                    tempChangeAddress = addressInflater.fromPrivateKey(privateKey, true);
                }

                final Transaction tempTransaction = wallet.createTransaction(paymentAmounts, tempChangeAddress, outputIdentifiersToSpend);
                if (tempTransaction == null) { break; }

                final Address changeAddress;
                final Long changeAmount = (tempTransaction.getTotalOutputValue() - amount);
                {
                    final PrivateKey privateKey = Main.derivePrivateKey(blockHeight, changeAmount);
                    changeAddress = addressInflater.fromPrivateKey(privateKey, true);
                }

                transaction = wallet.createTransaction(paymentAmounts, changeAddress, outputIdentifiersToSpend);
                if (transaction != null) {
                    final List<TransactionInput> transactionInputs = transaction.getTransactionInputs();
                    final List<TransactionOutput> transactionOutputs = transaction.getTransactionOutputs();
                    if (transactionOutputs.getCount() != 2) {
                        Logger.debug(transaction.getHash() + " walletBalance=" + walletBalance + " amount=" + amount + " changeAmount=" + changeAmount + " txInput=" + transactionInputs.getCount() + " txOutput=" + transactionOutputs.getCount() + " address=" + address + " changeAddress=" + changeAddress);
                        for (final TransactionOutput transactionOutput : transactionOutputs) {
                            Logger.debug("  -> " + transactionOutput.getAmount());
                        }
                    }

                    if (transactionOutputs.getCount() > 1) {
                        final TransactionOutput secondOutput = transactionOutputs.get(1);
                        if (! Util.areEqual(secondOutput.getAmount(), changeAmount)) {
                            Logger.warn("Change amount changed.");
                            transaction = null;
                        }
                    }
                }
                break;
            }

            if (transaction == null) {
                // minBalance += defaultMinBalance;

                for (final TestUtxo testUtxo : testUtxos) {
                    availableUtxos.add(testUtxo);
                }

                break;
            }

            final List<TransactionOutput> transactionOutputs = transaction.getTransactionOutputs();
            for (int i = 0; i < transactionOutputs.getCount(); ++i) {
                final TestUtxo utxo = new TestUtxo(transaction, i, blockHeight);
                availableUtxos.add(utxo);
            }

            final Integer byteCount = transactionDeflater.getByteCount(transaction);
            blockSize += byteCount;
            if (blockSize >= maxBlockSize) {
                Logger.debug("Max block size reached: " + blockSize + " of " + maxBlockSize);
                break;
            }
            else {
                transactions.add(transaction);
            }
        }

        if (unspendableCount > 0) {
            Logger.debug("Unspendable output count: " + unspendableCount);
        }

        final MutableList<Transaction> createdTransactions = new MutableList<>(transactions);
        Logger.debug("Created " + createdTransactions.getCount() + " transactions.");
        return createdTransactions;
    }

    public static MutableList<Transaction> createFanInTransactions(final List<TransactionWithBlockHeight> transactionsToSpend, final Long blockHeight) throws Exception {
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
