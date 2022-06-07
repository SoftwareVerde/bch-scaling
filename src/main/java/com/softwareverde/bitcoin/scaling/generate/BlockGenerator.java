package com.softwareverde.bitcoin.scaling.generate;

import com.softwareverde.bitcoin.CoreInflater;
import com.softwareverde.bitcoin.address.Address;
import com.softwareverde.bitcoin.address.AddressInflater;
import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockDeflater;
import com.softwareverde.bitcoin.block.CanonicalMutableBlock;
import com.softwareverde.bitcoin.block.MutableBlock;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.header.ImmutableBlockHeader;
import com.softwareverde.bitcoin.block.header.difficulty.Difficulty;
import com.softwareverde.bitcoin.inflater.MasterInflater;
import com.softwareverde.bitcoin.rpc.BitcoinMiningRpcConnector;
import com.softwareverde.bitcoin.rpc.BitcoinMiningRpcConnectorFactory;
import com.softwareverde.bitcoin.rpc.BlockTemplate;
import com.softwareverde.bitcoin.rpc.monitor.Monitor;
import com.softwareverde.bitcoin.scaling.BlockSender;
import com.softwareverde.bitcoin.scaling.DiskUtil;
import com.softwareverde.bitcoin.scaling.PrivateTestNetDifficultyCalculatorContext;
import com.softwareverde.bitcoin.scaling.generate.rpc.PrivateTestNetBitcoinMiningRpcConnector;
import com.softwareverde.bitcoin.stratum.callback.BlockFoundCallback;
import com.softwareverde.bitcoin.transaction.MutableTransaction;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.transaction.TransactionInflater;
import com.softwareverde.bitcoin.transaction.input.MutableTransactionInput;
import com.softwareverde.bitcoin.transaction.script.opcode.PushOperation;
import com.softwareverde.bitcoin.transaction.script.stack.Value;
import com.softwareverde.bitcoin.transaction.script.unlocking.MutableUnlockingScript;
import com.softwareverde.concurrent.threadpool.CachedThreadPool;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.constable.bytearray.MutableByteArray;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.cryptography.secp256k1.key.PrivateKey;
import com.softwareverde.json.Json;
import com.softwareverde.logging.Logger;
import com.softwareverde.util.Container;
import com.softwareverde.util.IoUtil;
import com.softwareverde.util.Util;
import com.softwareverde.util.timer.NanoTimer;
import com.softwareverde.util.type.time.SystemTime;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.math.BigInteger;
import java.util.HashSet;

public class BlockGenerator {
    protected BlockSender _blockSender;
    protected PrivateKeyGenerator _privateKeyGenerator;
    protected File _scenarioDirectory;
    protected boolean _shouldPerformProofOfWork = true;

    public BlockGenerator(final BlockSender blockSender, final PrivateKeyGenerator privateKeyGenerator, final File scenarioDirectory) {
        _blockSender = blockSender;
        _privateKeyGenerator = privateKeyGenerator;
        _scenarioDirectory = scenarioDirectory;
    }

    public void setProofOfWorkEnabled(boolean shouldPerformProofOfWork) {
        _shouldPerformProofOfWork = shouldPerformProofOfWork;
    }

    public MutableList<BlockHeader> generateBlocks(final Json blocksManifestJson, final long runningBlockHeight, final int targetNewBlockCount, final List<BlockHeader> blockHeaders, final TransactionGenerator transactionGenerator) {
        final MutableList<BlockHeader> scenarioBlocks = _loadBlocksFromManifest(blocksManifestJson, runningBlockHeight, targetNewBlockCount, _scenarioDirectory);
        _blockSender.sendBlocks(scenarioBlocks, runningBlockHeight, _scenarioDirectory, false);
        final int newBlockCount = (targetNewBlockCount - scenarioBlocks.getCount());
        scenarioBlocks.addAll(_generateBlocks(_privateKeyGenerator, newBlockCount, _scenarioDirectory, blockHeaders, transactionGenerator, _calculateTimestamp(blockHeaders)));
        return scenarioBlocks;
    }

    protected MutableList<BlockHeader> _loadBlocksFromManifest(final Json blocksManifestJson, final Long runningBlockHeight, final Integer targetNewBlockCount, final File blocksDirectory) {
        final MutableList<BlockHeader> blockHeaders = new MutableList<>(targetNewBlockCount);
        if (blocksManifestJson == null) { return blockHeaders; }

        final int mainNetBlockCount = (144 + 1); // Main Blocks + Genesis
        final long mainNetBlockHeight = (mainNetBlockCount - 1L);
        final int manifestBlockCount = blocksManifestJson.length();
        final long lastManifestBlockHeight = (manifestBlockCount + mainNetBlockHeight);

        final int blockCountToLoadFromManifest = Math.min(targetNewBlockCount, (int) Math.max(0L, (lastManifestBlockHeight - runningBlockHeight)));
        if (blockCountToLoadFromManifest == 0) { return blockHeaders; }

        final int manifestStartIndex = (int) (runningBlockHeight - mainNetBlockHeight);
        for (int i = 0; i < blockCountToLoadFromManifest; ++i) {
            final String blockHashString = blocksManifestJson.getString(manifestStartIndex + i);
            final Sha256Hash blockHash = Sha256Hash.fromHexString(blockHashString);
            final BlockHeader blockHeader = DiskUtil.loadBlockHeader(blockHash, blocksDirectory);
            if (blockHeader == null) {
                Logger.debug("Unable to load block from manifest: " + blockHash);
                break;
            }

            blockHeaders.add(blockHeader);
        }

        return blockHeaders;
    }

    protected Long _calculateTimestamp(final List<BlockHeader> blockHeaders) {
        final int blockHeaderCount = blockHeaders.getCount();
        final BlockHeader blockHeader = blockHeaders.get(blockHeaderCount - 1);
        final Long timestamp = blockHeader.getTimestamp();
        if (timestamp < 1640995200L) {
            return 1651113031L;
        }

        final long tenMinutes = 10L * 60L;
        return (timestamp + tenMinutes);
    }

    protected MutableList<BlockHeader> _generateBlocks(final PrivateKeyGenerator privateKeyGenerator, final Integer blockCount, final File blocksDirectory, final List<BlockHeader> initBlocks, final TransactionGenerator transactionGenerator, final Long timestamp) {
        if (blockCount > 0) {
            Logger.debug("Generating " + blockCount + " blocks.");
        }
        final AddressInflater addressInflater = new AddressInflater();
        final BlockDeflater blockDeflater = new BlockDeflater();
        final MasterInflater masterInflater = new CoreInflater();
        final CachedThreadPool threadPool = new CachedThreadPool(32, 10000L);

        final int initBlockCount = initBlocks.getCount();
        final BlockHeader lastInitBlock = initBlocks.get(initBlockCount - 1);
        final MutableList<BlockHeader> createdBlocks = new MutableList<>(blockCount);
        if (blockCount < 1) { return createdBlocks; }

        final PrivateTestNetDifficultyCalculatorContext difficultyCalculatorContext = new PrivateTestNetDifficultyCalculatorContext();
        difficultyCalculatorContext.addBlocks(initBlocks);

        final Container<BlockTemplate> blockTemplateContainer = new Container<>(null);

        final Container<StratumServer> stratumServerContainer = new Container<>();
        final Container<BlockFoundCallback> blockFoundCallbackContainer = new Container<>();
        final BitcoinMiningRpcConnectorFactory rpcConnectorFactory = new BitcoinMiningRpcConnectorFactory() {
            @Override
            public BitcoinMiningRpcConnector newBitcoinMiningRpcConnector() {
                return new PrivateTestNetBitcoinMiningRpcConnector(blockTemplateContainer) {
                    @Override
                    public Boolean submitBlock(final Block block, final Monitor monitor) {
                        final BlockHeader lastCreatedBlock;
                        synchronized (createdBlocks) {
                            final int createdBlockCount = createdBlocks.getCount();
                            lastCreatedBlock = ((createdBlockCount < 1) ? lastInitBlock : createdBlocks.get(createdBlockCount - 1));
                        }

                        final Sha256Hash blockHash = block.getHash();
                        final Sha256Hash expectedPreviousBlockHash = lastCreatedBlock.getHash();
                        final Sha256Hash previousBlockHash = block.getPreviousBlockHash();

                        if (! Util.areEqual(expectedPreviousBlockHash, previousBlockHash)) {
                            // Logger.debug("Rejected: " + blockHash + " " + expectedPreviousBlockHash + " != " + previousBlockHash);
                            return false;
                        }

                        final StratumServer stratumServer = stratumServerContainer.value;
                        if (stratumServer != null) {
                            stratumServer.abandonMiningTasks();
                        }

                        synchronized (blockTemplateContainer) {
                            if (blockTemplateContainer.value != null) {
                                blockTemplateContainer.value = null;

                                (new Thread(new Runnable() {
                                    @Override
                                    public void run() {
                                        blockFoundCallbackContainer.value.run(block, "");
                                    }
                                })).start();
                            }
                        }

                        return super.submitBlock(block, monitor);
                    }
                };
            }
        };

        final SystemTime systemTime = new SystemTime() {
            @Override
            public Long getCurrentTimeInSeconds() {
                final int newBlockCount = createdBlocks.getCount();
                return timestamp + (newBlockCount * 10L * 60L);
            }

            @Override
            public Long getCurrentTimeInMilliSeconds() {
                return (this.getCurrentTimeInSeconds() * 1000L);
            }
        };

        final StratumServer stratumServer = new StratumServer(rpcConnectorFactory, 3333, threadPool, masterInflater, systemTime);
        stratumServerContainer.value = stratumServer;

        {
            final int blocksCount = initBlocks.getCount();
            final BlockHeader previousBlock = initBlocks.get(blocksCount - 1);
            final Sha256Hash previousBlockHash = previousBlock.getHash();

            final NanoTimer nanoTimer = new NanoTimer();
            nanoTimer.start();

            final Long nextBlockHeight = (long) blocksCount;
            // final List<Transaction> transactions = ((transactionGenerator != null) ? transactionGenerator.getTransactions(nextBlockHeight) : new MutableList<>(0));
            blockTemplateContainer.value = GenerationUtil.getBlockTemplate(blocksDirectory, previousBlockHash, nextBlockHeight, transactionGenerator, difficultyCalculatorContext, initBlocks, createdBlocks); // _createBlockTemplate(previousBlockHash, nextBlockHeight, transactions, difficultyCalculatorContext);

            nanoTimer.stop();
            Logger.debug("Template generated in: " + nanoTimer.getMillisecondsElapsed() + "ms.");

            final PrivateKey privateKey = privateKeyGenerator.getCoinbasePrivateKey(nextBlockHeight);
            final Address address = addressInflater.fromPrivateKey(privateKey, true);
            stratumServer.setCoinbaseAddress(address);
        }

        if (! blocksDirectory.exists()) { blocksDirectory.mkdirs(); }

        final BlockFoundCallback blockFoundCallback = new BlockFoundCallback() {
            @Override
            public void run(final Block block, final String workerName) {
                final int createdBlockCount;
                synchronized (createdBlocks) {
                    createdBlockCount = createdBlocks.getCount();
                    if (createdBlockCount >= blockCount) {
                        createdBlocks.notifyAll();
                        return;
                    }
                }

                final Sha256Hash blockHash = block.getHash();
                final ByteArray blockBytes = blockDeflater.toBytes(block);

                final File file = new File(blocksDirectory, blockHash.toString());
                IoUtil.putFileContents(file, blockBytes);

                final BlockHeader blockHeader = new ImmutableBlockHeader(block);
                final int newCreatedBlockCount;
                synchronized (createdBlocks) {
                    createdBlocks.add(blockHeader);
                    newCreatedBlockCount = createdBlocks.getCount();
                }
                difficultyCalculatorContext.addBlock(blockHeader);

                final long blockHeight = ( (initBlockCount - 1) + (createdBlockCount + 1) );
                Logger.info("Height: " + blockHeight + " " + blockHash + " " + file.getPath() + " byteCount=" + blockDeflater.getByteCount(block));

                _blockSender.sendBlock(block, blockHeight, blocksDirectory, true);

                if (newCreatedBlockCount >= blockCount) {
                    synchronized (createdBlocks) {
                        createdBlocks.notifyAll();
                        return;
                    }
                }

                final long nextBlockHeight = (blockHeight + 1L);
                // final List<Transaction> transactions = ((transactionGenerator != null) ? transactionGenerator.getTransactions(nextBlockHeight) : new MutableList<>(0));
                blockTemplateContainer.value = GenerationUtil.getBlockTemplate(blocksDirectory, blockHash, nextBlockHeight, transactionGenerator, difficultyCalculatorContext, initBlocks, createdBlocks); // _createBlockTemplate(blockHash, nextBlockHeight, transactions, difficultyCalculatorContext);
                synchronized (blockTemplateContainer) {
                    blockTemplateContainer.notifyAll();
                }

                final PrivateKey privateKey = privateKeyGenerator.getCoinbasePrivateKey(nextBlockHeight);
                final Address address = addressInflater.fromPrivateKey(privateKey, true);
                stratumServer.setCoinbaseAddress(address);
                stratumServer.rebuildBlockTemplate();
            }
        };
        blockFoundCallbackContainer.value = blockFoundCallback;

        if (!_shouldPerformProofOfWork) {
            while (true) {
                final Long blockHeight = blockTemplateContainer.value.getBlockHeight();
                final MutableBlock block = blockTemplateContainer.value.toBlock();
                {
                    final PrivateKey privateKey = privateKeyGenerator.getCoinbasePrivateKey(blockHeight);
                    final Address address = addressInflater.fromPrivateKey(privateKey, true);

                    final TransactionInflater transactionInflater = new TransactionInflater();

                    final Transaction dummyCoinbaseTransaction = block.getCoinbaseTransaction();
                    final Long amount = dummyCoinbaseTransaction.getTotalOutputValue();
                    final Transaction coinbaseTransaction = transactionInflater.createCoinbaseTransaction(blockHeight, "", address, amount);

                    block.replaceTransaction(0, coinbaseTransaction);
                }

                blockFoundCallback.run(block, "");
                if (blockTemplateContainer.value == null) { break; }

                final Long newBlockHeight = blockTemplateContainer.value.getBlockHeight();
                if (Util.areEqual(blockHeight, newBlockHeight)) { break; }
            }
            return createdBlocks;
        }

        threadPool.start();
        stratumServer.start();

        int createdBlockCount = 0;
        while (createdBlockCount < blockCount) {
            synchronized (createdBlocks) {
                try {
                    createdBlocks.wait();
                }
                catch (final Exception exception) { break; }
                createdBlockCount = createdBlocks.getCount();
            }
        }

        stratumServer.stop();
        threadPool.stop();

        Logger.debug("Generated " + createdBlockCount + " blocks.");

        return createdBlocks;
    }

    public void createReorgBlock(final Sha256Hash blockHash, final Long blockHeight, final File defaultDirectory) throws Exception {
        final HashSet<Sha256Hash> blockTransactions = new HashSet<>();
        {
            final Block block = DiskUtil.loadBlock(blockHash, defaultDirectory);

            final CanonicalMutableBlock mutableBlock = new CanonicalMutableBlock(block);

            // chainedTransactionHashes should contain any transactions that must not be removed from the block because they are depended on by another transaction within that block...
            final HashSet<Sha256Hash> chainedTransactionHashes = new HashSet<>();
            // chainedTransactionHashes.add(Sha256Hash.fromHexString("ABC1F2D110B6F351DD637195543BD7E6034C419E8F76834DF950108865B4EDE1"));

            final int transactionCount = mutableBlock.getTransactionCount();
            final int txToRemoveCount = (int) (0.005 * transactionCount);
            int ix = (transactionCount - 1);
            int removedCount = 0;
            while (removedCount < txToRemoveCount) {
                final List<Transaction> transactions = mutableBlock.getTransactions();
                final Sha256Hash txHash = transactions.get(ix).getHash();
                if (! chainedTransactionHashes.contains(txHash)) { // if (! Util.areEqual(txHash, chainedTransactionHash)) {
                    mutableBlock.removeTransaction(txHash);
                    removedCount += 1;
                }
                ix -= 1;
            }
            System.out.println("Removed " + removedCount + " transactions.");

            boolean isCoinbase = true;
            for (final Transaction transaction : mutableBlock.getTransactions()) {
                if (isCoinbase) {
                    isCoinbase = false;
                    continue;
                }
                blockTransactions.add(transaction.getHash());
            }

            final MutableTransaction coinbaseTransaction = new MutableTransaction(mutableBlock.getCoinbaseTransaction());
            final MutableTransactionInput transactionInput = new MutableTransactionInput(coinbaseTransaction.getTransactionInputs().get(0));
            final MutableUnlockingScript unlockingScript = new MutableUnlockingScript(transactionInput.getUnlockingScript());

            long nonce = 0L;
            long extraNonce = 1L;

            final NanoTimer nanoTimer = new NanoTimer();
            nanoTimer.start();

            final Difficulty difficulty = mutableBlock.getDifficulty();
            while (true) {
                final boolean rebuildCoinbase;
                if (nonce >= Integer.MAX_VALUE - 1L) {
                    rebuildCoinbase = true;
                    nonce = 0L;
                    extraNonce += 1L;
                }
                else {
                    rebuildCoinbase = false;
                    nonce += 1L;
                }

                final Sha256Hash newBlockHash = mutableBlock.getHash();

                if (nonce % 1000000 == 0) {
                    final Double msElapsed = nanoTimer.getMillisecondsElapsed();
                    final BigInteger bigInteger = BigInteger.valueOf(nonce).multiply(BigInteger.valueOf(extraNonce));
                    System.out.println(bigInteger + " hashes in " + msElapsed + " (" + (bigInteger.divide(BigInteger.valueOf(msElapsed.longValue()))) + "h/s) " + newBlockHash + " " + difficulty.getDifficultyRatio());
                }

                if (difficulty.isSatisfiedBy(newBlockHash)) {
                    final BlockDeflater blockDeflater = new BlockDeflater();
                    final ByteArray blockBytes = blockDeflater.toBytes(mutableBlock);

                    final File outputFile = new File(defaultDirectory, newBlockHash.toString());
                    IoUtil.putFileContents(outputFile, blockBytes);

                    System.out.println(outputFile.getAbsolutePath());
                    break;
                }

                if (rebuildCoinbase) {
                    unlockingScript.removeOperation(3);
                    unlockingScript.addOperation(PushOperation.pushValue(Value.fromInteger(extraNonce)));
                    transactionInput.setUnlockingScript(unlockingScript);
                    coinbaseTransaction.setTransactionInput(0, transactionInput);
                    mutableBlock.replaceTransaction(0, coinbaseTransaction);
                }
                mutableBlock.setNonce(nonce);
            }
        }

        final File mempoolInputFile;
        {
            mempoolInputFile = new File(defaultDirectory, "/mempool/" + blockHeight + "-main-chain.sha");
            final File originalFile = new File(defaultDirectory, "/mempool/" + blockHeight + ".sha");
            originalFile.renameTo(mempoolInputFile);
        }

        final File mempoolOutputFile = new File(defaultDirectory, "/mempool/" + blockHeight + ".sha");
        final MutableByteArray readBuffer = new MutableByteArray(Sha256Hash.BYTE_COUNT);
        try (
                final FileInputStream inputStream = new FileInputStream(mempoolInputFile);
                final FileOutputStream outputStream = new FileOutputStream(mempoolOutputFile)
        ) {
            while (true) {
                final int readByteCount = inputStream.read(readBuffer.unwrap());
                if (readByteCount != Sha256Hash.BYTE_COUNT) { break; }

                final Sha256Hash transactionHash = Sha256Hash.copyOf(readBuffer.unwrap());
                if (blockTransactions.contains(transactionHash)) {
                    outputStream.write(readBuffer.unwrap());
                }
            }
            outputStream.flush();
        }
        catch (final Exception exception) {
            Logger.debug(exception);
        }
    }
}