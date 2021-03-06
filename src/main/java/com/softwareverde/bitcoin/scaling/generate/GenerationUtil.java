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
import com.softwareverde.bitcoin.server.database.BatchRunner;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.transaction.TransactionDeflater;
import com.softwareverde.bitcoin.transaction.coinbase.CoinbaseTransaction;
import com.softwareverde.bitcoin.transaction.input.TransactionInput;
import com.softwareverde.bitcoin.transaction.output.TransactionOutput;
import com.softwareverde.bitcoin.wallet.*;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.cryptography.secp256k1.key.PrivateKey;
import com.softwareverde.logging.Logger;
import com.softwareverde.util.ByteUtil;
import com.softwareverde.util.IoUtil;
import com.softwareverde.util.Util;
import com.softwareverde.util.timer.NanoTimer;
import com.softwareverde.util.type.time.SystemTime;

import java.io.File;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class GenerationUtil {
    protected GenerationUtil() { }

    public static BlockTemplate getBlockTemplate(final File blocksDirectory, final Sha256Hash previousBlockHash, final Long nextBlockHeight, final TransactionGenerator transactionGenerator, final PrivateTestNetDifficultyCalculatorContext difficultyCalculatorContext, final List<BlockHeader> existingBlockHeaders, final List<BlockHeader> createdBlocks) {
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

        final List<Transaction> transactions = ((transactionGenerator != null) ? transactionGenerator.getTransactions(nextBlockHeight, existingBlockHeaders, createdBlocks) : new MutableList<>(0));
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

    public static class DebugWallet extends Wallet {
        public void debugWalletState() {
            _debugWalletState();
        }
    }

    public static MutableList<Transaction> createCashTransactions(final Transaction optionalTransaction, final Deque<SpendableTransactionOutput> availableUtxos, final Long blockHeight) throws Exception {
        final long maxBlockSize = 256L * ByteUtil.Unit.Si.MEGABYTES;

        final AddressInflater addressInflater = new AddressInflater();

        final NanoTimer nanoTimer = new NanoTimer();

        nanoTimer.start();
        final ConcurrentLinkedQueue<Transaction> transactions = new ConcurrentLinkedQueue<>();

        final TransactionDeflater transactionDeflater = new TransactionDeflater();

        final long coinbaseSize = 2048; // Over-estimate.
        long blockSize = (BlockHeaderInflater.BLOCK_HEADER_BYTE_COUNT + 8L);
        blockSize += coinbaseSize;

        if (optionalTransaction != null) {
            blockSize += transactionDeflater.getByteCount(optionalTransaction);
        }

        // final long defaultMinBalance = 100000L;
        // long minBalance = defaultMinBalance;
        final long minBalance = 100000L;

        // long availableBalance = 0L;
        // for (final SpendableTransactionOutput spendableTransactionOutput : availableUtxos) {
        //     availableBalance += spendableTransactionOutput.getAmount();
        // }
        // Logger.debug(availableUtxos.getCount() + " UTXOs available, with " + availableBalance + " satoshis total available.");
        Logger.debug(availableUtxos.size() + " UTXOs available.");

        final Address tempChangeAddress;
        {
            final PrivateKey privateKey = PrivateKey.fromHexString("0000000000000000000000000000000000000000000000000000000000000001");
            tempChangeAddress = addressInflater.fromPrivateKey(privateKey, true);
        }

        int iterationCount = 0;
        int unspendableCount = 0;
        while (blockSize < maxBlockSize) {
            iterationCount += 1;
            if (iterationCount % 1024 == 0) {
                Logger.debug("iterationCount=" + iterationCount + " blockSize=" + blockSize + " availableUtxos.count=" + availableUtxos.size()); // + " transactions.count=" + transactions.size());
            }

            if (availableUtxos.isEmpty()) { break; }

            final long minOutputAmount = 546;

            final SlimWallet wallet = new SlimWallet();

            int outputsToSpendCount = 0;
            long spendableWalletBalance = 0L;
            while ((spendableWalletBalance < minBalance) || (outputsToSpendCount < 2)) {
                if (availableUtxos.isEmpty()) { break; }

                final SpendableTransactionOutput spendableTransactionOutput = availableUtxos.removeFirst();
                final PrivateKey privateKey = Main.derivePrivateKey(spendableTransactionOutput.getBlockHeight(), spendableTransactionOutput.getAmount());

                final boolean isSpendable;
                {
                    final SlimWallet isSpendableWallet = new SlimWallet();
                    isSpendableWallet.addPrivateKey(privateKey);
                    isSpendableWallet.addUnspentTransactionOutput(spendableTransactionOutput);
                    isSpendable = (isSpendableWallet.getBalance() > 0L);
                }
                if (! isSpendable) {
                    unspendableCount += 1;
                    continue;
                }

                wallet.addPrivateKey(privateKey);
                spendableWalletBalance += spendableTransactionOutput.getAmount();

                wallet.addUnspentTransactionOutput(spendableTransactionOutput);

                outputsToSpendCount += 1;
            }

            if (wallet.getBalance() < minBalance) {
                Logger.debug("Unable to load min balance: " + wallet.getBalance());
                continue;
            }

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

                final Transaction tempTransaction = wallet.createTransaction(paymentAmounts, tempChangeAddress);
                if (tempTransaction == null) { break; }

                final Address changeAddress;
                final Long changeAmount = (tempTransaction.getTotalOutputValue() - amount);
                {
                    final PrivateKey privateKey = Main.derivePrivateKey(blockHeight, changeAmount);
                    changeAddress = addressInflater.fromPrivateKey(privateKey, true);
                }

                transaction = wallet.createTransaction(paymentAmounts, changeAddress);
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
                for (final SpendableTransactionOutput spendableTransactionOutput : wallet.getUnspentTransactionOutputs()) {
                    // availableUtxos.addAll(wallet.getUnspentTransactionOutputs());
                    availableUtxos.add(spendableTransactionOutput);
                }
                break;
            }

            final Integer byteCount = transaction.getByteCount();
            if ((blockSize + byteCount) >= maxBlockSize) {
                Logger.debug("Max block size reached: " + blockSize + " of " + maxBlockSize);
                break;
            }
            else {
                blockSize += byteCount;
                transactions.add(transaction);

                // Add the UTXOs from this transaction into the UTXO pool...
                final Sha256Hash transactionHash = transaction.getHash();
                final List<TransactionOutput> transactionOutputs = transaction.getTransactionOutputs();
                for (int i = 0; i < transactionOutputs.getCount(); ++i) {
                    final TransactionOutput transactionOutput = transactionOutputs.get(i);
                    final SpendableTransactionOutput utxo = new WalletUtxo(transactionOutput, transactionHash, blockHeight, false);
                    availableUtxos.add(utxo);
                }
            }
        }

        if (unspendableCount > 0) {
            Logger.debug("Unspendable output count: " + unspendableCount);
        }

        final MutableList<Transaction> createdTransactions = new MutableList<>(transactions);
        Logger.debug("Created " + createdTransactions.getCount() + " transactions.");
        return createdTransactions;
    }
}