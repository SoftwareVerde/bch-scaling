package com.softwareverde.bitcoin.scaling;

import com.softwareverde.bitcoin.CoreInflater;
import com.softwareverde.bitcoin.address.Address;
import com.softwareverde.bitcoin.address.AddressInflater;
import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockDeflater;
import com.softwareverde.bitcoin.block.BlockInflater;
import com.softwareverde.bitcoin.block.MutableBlock;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.header.BlockHeaderWithTransactionCount;
import com.softwareverde.bitcoin.block.header.ImmutableBlockHeader;
import com.softwareverde.bitcoin.block.header.ImmutableBlockHeaderWithTransactionCount;
import com.softwareverde.bitcoin.inflater.MasterInflater;
import com.softwareverde.bitcoin.rpc.BitcoinMiningRpcConnector;
import com.softwareverde.bitcoin.rpc.BitcoinMiningRpcConnectorFactory;
import com.softwareverde.bitcoin.rpc.BitcoinNodeRpcAddress;
import com.softwareverde.bitcoin.rpc.BitcoinVerdeRpcConnector;
import com.softwareverde.bitcoin.rpc.BlockTemplate;
import com.softwareverde.bitcoin.rpc.RpcCredentials;
import com.softwareverde.bitcoin.rpc.core.BitcoinCoreRpcConnector;
import com.softwareverde.bitcoin.rpc.monitor.Monitor;
import com.softwareverde.bitcoin.scaling.generate.GenerationUtil;
import com.softwareverde.bitcoin.scaling.generate.StratumServer;
import com.softwareverde.bitcoin.scaling.generate.rpc.PrivateTestNetBitcoinMiningRpcConnector;
import com.softwareverde.bitcoin.scaling.rpc.BitcoinCoreRpcConnector2;
import com.softwareverde.bitcoin.scaling.rpc.TransactionRpcConnector;
import com.softwareverde.bitcoin.scaling.rpc.VerdeTransactionRpcConnector;
import com.softwareverde.bitcoin.server.main.BitcoinConstants;
import com.softwareverde.bitcoin.server.message.type.node.feature.LocalNodeFeatures;
import com.softwareverde.bitcoin.server.message.type.node.feature.NodeFeatures;
import com.softwareverde.bitcoin.server.message.type.query.response.hash.InventoryItem;
import com.softwareverde.bitcoin.server.message.type.query.response.hash.InventoryItemType;
import com.softwareverde.bitcoin.server.node.BitcoinNode;
import com.softwareverde.bitcoin.stratum.callback.BlockFoundCallback;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.transaction.TransactionInflater;
import com.softwareverde.bitcoin.transaction.input.TransactionInput;
import com.softwareverde.bitcoin.transaction.output.TransactionOutput;
import com.softwareverde.bitcoin.wallet.PaymentAmount;
import com.softwareverde.bitcoin.wallet.Wallet;
import com.softwareverde.concurrent.ConcurrentHashSet;
import com.softwareverde.concurrent.Pin;
import com.softwareverde.concurrent.threadpool.CachedThreadPool;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.constable.bytearray.MutableByteArray;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.immutable.ImmutableList;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.cryptography.hash.sha256.MutableSha256Hash;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.cryptography.secp256k1.key.PrivateKey;
import com.softwareverde.http.HttpResponse;
import com.softwareverde.http.WebRequest;
import com.softwareverde.json.Json;
import com.softwareverde.logging.LineNumberAnnotatedLog;
import com.softwareverde.logging.LogLevel;
import com.softwareverde.logging.Logger;
import com.softwareverde.network.p2p.node.Node;
import com.softwareverde.util.ByteUtil;
import com.softwareverde.util.Container;
import com.softwareverde.util.IoUtil;
import com.softwareverde.util.StringUtil;
import com.softwareverde.util.Tuple;
import com.softwareverde.util.Util;
import com.softwareverde.util.timer.NanoTimer;
import com.softwareverde.util.type.time.SystemTime;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

public class Main implements AutoCloseable {
    public static final Boolean SKIP_SEND = false;
    public static final Boolean SKIP_MINING = false;
    public static final Float PRE_RELAY_PERCENT = 0F;
    public static final Boolean USE_P2P_BROADCAST = false;
    public static final Boolean IS_BITCOIN_VERDE_NODE = false;

    public static void main(final String[] commandLineArguments) {
        BitcoinConstants.setBlockMaxByteCount((int) (256L * ByteUtil.Unit.Si.MEGABYTES));

        Logger.setLog(LineNumberAnnotatedLog.getInstance());
        Logger.setLogLevel(LogLevel.DEBUG);
        Logger.setLogLevel("com.softwareverde.util", LogLevel.ERROR);
        Logger.setLogLevel("com.softwareverde.network", LogLevel.INFO);
        Logger.setLogLevel("com.softwareverde.async.lock", LogLevel.WARN);
        Logger.setLogLevel("com.softwareverde.bitcoin.stratum.BitcoinCoreStratumServer", LogLevel.WARN);
        Logger.setLogLevel("com.softwareverde.bitcoin.wallet.Wallet", LogLevel.WARN);

        try (final Main main = new Main()) {
            main.run();
        }
    }

    public static Sha256Hash getBlockHash(final Long blockHeight, final List<BlockHeader> blockHeaders, final List<BlockHeader> newBlockHeaders) {
        if (blockHeight < blockHeaders.getCount()) {
            final int blockHeightInt = blockHeight.intValue();
            final BlockHeader blockHeader = blockHeaders.get(blockHeightInt);
            return blockHeader.getHash();
        }

        final int index = (int) (blockHeight - blockHeaders.getCount());
        final BlockHeader blockHeader = newBlockHeaders.get(index);
        return blockHeader.getHash();
    }

    public static PrivateKey derivePrivateKey(final Long blockHeight, final Long outputValue) {
        final MutableByteArray bytes = new MutableByteArray(PrivateKey.KEY_BYTE_COUNT);
        final byte[] blockHeightBytes = ByteUtil.longToBytes(blockHeight);
        final byte[] outputValueBytes = ByteUtil.longToBytes(outputValue);

        final int startIndex = (PrivateKey.KEY_BYTE_COUNT - blockHeightBytes.length - outputValueBytes.length);
        bytes.setBytes(startIndex, blockHeightBytes);
        bytes.setBytes(startIndex + blockHeightBytes.length, outputValueBytes);

        return PrivateKey.fromBytes(bytes);
    }

    protected MutableList<BlockHeader> _generateBlocks(final PrivateKeyGenerator privateKeyGenerator, final Integer blockCount, final File blocksDirectory, final List<BlockHeader> initBlocks, final Long timestamp) {
        return _generateBlocks(privateKeyGenerator, blockCount, blocksDirectory, initBlocks, null, timestamp);
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
            blockTemplateContainer.value = GenerationUtil.getBlockTemplate(blocksDirectory, previousBlockHash, nextBlockHeight, transactionGenerator, difficultyCalculatorContext, createdBlocks); // _createBlockTemplate(previousBlockHash, nextBlockHeight, transactions, difficultyCalculatorContext);

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

                _sendBlock(block, blockHeight, blocksDirectory, true);

                if (newCreatedBlockCount >= blockCount) {
                    synchronized (createdBlocks) {
                        createdBlocks.notifyAll();
                        return;
                    }
                }

                final long nextBlockHeight = (blockHeight + 1L);
                // final List<Transaction> transactions = ((transactionGenerator != null) ? transactionGenerator.getTransactions(nextBlockHeight) : new MutableList<>(0));
                blockTemplateContainer.value = GenerationUtil.getBlockTemplate(blocksDirectory, blockHash, nextBlockHeight, transactionGenerator, difficultyCalculatorContext, createdBlocks); // _createBlockTemplate(blockHash, nextBlockHeight, transactions, difficultyCalculatorContext);
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

        if (SKIP_MINING) {
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


    protected void _sendBlockHeader(final BlockHeader block, final Integer transactionCount) {
        final Sha256Hash blockHash = block.getHash();
        _currentBlockHash = blockHash;

        if (SKIP_SEND) {
            Logger.debug("Skipped Header: " + blockHash);
            return;
        }

        final BlockHeaderWithTransactionCount blockHeader = new ImmutableBlockHeaderWithTransactionCount(block, transactionCount);
        _bitcoinNode.transmitBlockHeader(blockHeader);

        Logger.debug("Sent Header: " + blockHash);
    }

    protected void _sendBlock(final Block block, final Long blockHeight, final File scenarioDirectory, final Boolean shouldWait) {
        final Sha256Hash blockHash = block.getHash();
        // _currentBlockHash = blockHash;

        if (SKIP_SEND) {
            Logger.debug("Skipped Sending Of: " + blockHash);
            return;
        }

        if ( (blockHeight != null) && (PRE_RELAY_PERCENT > 0F) ) {
            final File directory = new File(scenarioDirectory, "mempool");
            final File file = new File(directory, blockHeight + ".sha");
            if (file.exists()) {
                final int blockTransactionCount = block.getTransactionCount();
                final int transactionCount = (int) ((blockTransactionCount - 1) * PRE_RELAY_PERCENT);
                final int seconds = (10 * 60);
                final int batchSizePerSecond = (transactionCount / seconds);

                int sentTransactionsCount = 0;
                if (USE_P2P_BROADCAST) {
                    _transactionsBeingServed.clear();
                    for (final Transaction transaction : block.getTransactions()) {
                        final Sha256Hash transactionHash = transaction.getHash();
                        _transactionsBeingServed.put(transactionHash, transaction);
                    }

                    Logger.debug("Starting Transactions: " + blockHash + " " + blockHeight);

                    final MutableByteArray readBuffer = new MutableByteArray(Sha256Hash.BYTE_COUNT);
                    try (final FileInputStream inputStream = new FileInputStream(file)) {
                        while (sentTransactionsCount < transactionCount) {
                            final NanoTimer nanoTimer = new NanoTimer();
                            nanoTimer.start();

                            final MutableList<Sha256Hash> transactionHashesBatch = new MutableList<>();
                            for (int i = 0; i < batchSizePerSecond; ++i) {
                                final int readByteCount = inputStream.read(readBuffer.unwrap());
                                if (readByteCount != Sha256Hash.BYTE_COUNT) { break; }

                                final Sha256Hash transactionHash = Sha256Hash.copyOf(readBuffer.unwrap());
                                transactionHashesBatch.add(transactionHash);

                                sentTransactionsCount += 1;
                            }

                            if (transactionHashesBatch.isEmpty()) { break; }

                            _bitcoinNode.transmitTransactionHashes(transactionHashesBatch);

                            nanoTimer.stop();

                            final long delayMs = (long) (1000L - nanoTimer.getMillisecondsElapsed());
                            if (delayMs >= 1L) {
                                Thread.sleep(delayMs);
                            }
                        }
                    }
                    catch (final Exception exception) {
                        Logger.debug(exception);
                    }
                }
                else {
                    final List<Transaction> transactions = block.getTransactions();
                    final HashMap<Sha256Hash, Transaction> transactionHashMap = new HashMap<>(transactions.getCount());

                    for (final Transaction transaction : transactions) {
                        final Sha256Hash transactionHash = transaction.getHash();
                        transactionHashMap.put(transactionHash, transaction);
                    }
                    final long delayBetweenTransaction = (transactionCount > 0 ? (1000L / batchSizePerSecond) : 0);

                    try (final TransactionRpcConnector transactionRpcConnector = (IS_BITCOIN_VERDE_NODE ? new VerdeTransactionRpcConnector(_rpcAddress) : new BitcoinCoreRpcConnector2(_rpcAddress, _rpcCredentials))) {
                        Logger.debug("Starting Transactions: " + blockHash + " " + blockHeight);
                        final MutableSha256Hash transactionHash = new MutableSha256Hash();
                        try (final FileInputStream inputStream = new FileInputStream(file)) {
                            while (sentTransactionsCount < transactionCount) {
                                final NanoTimer nanoTimer = new NanoTimer();
                                nanoTimer.start();

                                final int readByteCount = inputStream.read(transactionHash.unwrap());
                                if (readByteCount != Sha256Hash.BYTE_COUNT) { break; }

                                final Transaction transaction = transactionHashMap.get(transactionHash);
                                if (transaction == null) { continue; }

                                transactionRpcConnector.submitTransaction(transaction);
                                sentTransactionsCount += 1;

                                nanoTimer.stop();

                                // if (Logger.isDebugEnabled()) {
                                //     Logger.debug(transaction.getHash() + " - " + nanoTimer.getMillisecondsElapsed() + "ms");
                                // }

                                final long delayMs = (long) (delayBetweenTransaction - nanoTimer.getMillisecondsElapsed());
                                if (delayMs >= 1L) {
                                    Thread.sleep(delayMs);
                                }
                            }
                        }
                    }
                    catch (final Exception exception) {
                        Logger.debug(exception);
                    }
                }
            }
        }

        Logger.debug("Sending Full Block: " + blockHash + " " + blockHeight);

        if (USE_P2P_BROADCAST) {
            // _bitcoinNode.transmitBlockHashes(new ImmutableList<>(blockHash));
            final Integer transactionCount = block.getTransactionCount();
            final BlockHeaderWithTransactionCount blockHeader = new ImmutableBlockHeaderWithTransactionCount(block, transactionCount);
            _bitcoinNode.transmitBlockHeader(blockHeader);

            if (shouldWait) {
                synchronized (_servedBlocks) {
                    while (! _servedBlocks.contains(blockHash)) {
                        try {
                            _servedBlocks.wait(); // Wait for the block to be downloaded...
                        }
                        catch (final Exception exception) {
                            final Thread thread = Thread.currentThread();
                            thread.interrupt();
                            return;
                        }
                    }
                }
            }
        }
        else {
            try (final BitcoinMiningRpcConnector bitcoinRpcConnector = (IS_BITCOIN_VERDE_NODE ? new BitcoinVerdeRpcConnector(_rpcAddress, _rpcCredentials) : new BitcoinCoreRpcConnector(_rpcAddress, _rpcCredentials))) {
                bitcoinRpcConnector.submitBlock(block);
            }
        }

        Logger.debug("Sent: " + blockHash);
    }

    protected final BitcoinNodeRpcAddress _rpcAddress;
    protected final RpcCredentials _rpcCredentials;
    protected final Sha256Hash _genesisBlockHash = Sha256Hash.fromHexString(BitcoinConstants.getGenesisBlockHash());
    protected CachedThreadPool _threadPool;
    protected BitcoinNode _bitcoinNode;
    protected final ConcurrentHashMap<Sha256Hash, Block> _initBlocks = new ConcurrentHashMap<>();
    protected final ConcurrentHashSet<Sha256Hash> _servedBlocks = new ConcurrentHashSet<>();
    protected Sha256Hash _currentBlockHash;
    protected ConcurrentHashMap<Sha256Hash, Transaction> _transactionsBeingServed = new ConcurrentHashMap<>();

    protected Tuple<MutableList<BlockHeader>, MutableList<Block>> _loadInitBlocks(final File blocksBaseDirectory) {
        final MutableList<BlockHeader> initBlockHeaders = new MutableList<>();
        final MutableList<Block> initBlocks = new MutableList<>();

        final BlockInflater blockInflater = new BlockInflater();
        final Json mainNetBlocks = Json.parse(IoUtil.getResource("/manifest.json")); // Genesis through Block #144 (inclusive)
        for (int i = 0; i < mainNetBlocks.length(); ++i) {
            final String blockHash = mainNetBlocks.getString(i);
            final File blockDirectory = new File(blocksBaseDirectory, blockHash);

            final Block block;
            final byte[] cachedBlockBytes = IoUtil.getFileContents(blockDirectory);
            if (cachedBlockBytes != null) {
                block = blockInflater.fromBytes(cachedBlockBytes);
            }
            else {
                final WebRequest webRequest = new WebRequest();
                webRequest.setUrl("https://explorer.bitcoinverde.org/api/v1/search");
                webRequest.addGetParam("query", blockHash);
                webRequest.addGetParam("rawFormat", "1");

                final HttpResponse response = webRequest.execute();
                final Json result = response.getJsonResult();

                final String blockHex = result.getString("object");
                final ByteArray blockBytes = ByteArray.fromHexString(blockHex);
                block = blockInflater.fromBytes(blockBytes);

                IoUtil.putFileContents(blockDirectory, blockBytes);
            }

            final BlockHeader blockHeader = new ImmutableBlockHeader(block);
            initBlockHeaders.add(blockHeader);
            initBlocks.add(block);

            if (! USE_P2P_BROADCAST) {
                _sendBlock(block, null, null, false);
            }
        }

        Logger.debug(initBlockHeaders.getCount() + " blocks loaded.");
        Logger.debug("HeadBlock=" + initBlockHeaders.get(initBlockHeaders.getCount() - 1).getHash());

        return new Tuple<>(initBlockHeaders, initBlocks);
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

    protected void _writeTransactionGenerationOrder(final Transaction transaction, final List<Transaction> transactions, final File defaultScenarioDirectory, final Long blockHeight) {
        final File directory = new File(defaultScenarioDirectory, "mempool");
        if (! directory.exists()) {
            directory.mkdirs();
        }

        final File file = new File(directory, blockHeight + ".sha");
        try (final FileOutputStream fileOutputStream = new FileOutputStream(file, false)) {
            if (transaction != null) {
                final Sha256Hash transactionHash = transaction.getHash();
                fileOutputStream.write(transactionHash.getBytes());
            }
            for (final Transaction tx : transactions) {
                final Sha256Hash transactionHash = tx.getHash();
                fileOutputStream.write(transactionHash.getBytes());
            }
        }
        catch (final Exception exception) {
            Logger.debug(exception);
        }
    }

    protected Transaction _splitCoinbaseTransaction(final Long blockHeight, final Long coinbaseToSpendBlockHeight, final List<BlockHeader> blockHeaders, final File defaultScenarioDirectory, final PrivateKeyGenerator privateKeyGenerator) {
        final BlockHeader blockHeader = blockHeaders.get(coinbaseToSpendBlockHeight.intValue());
        Logger.debug("Splitting Coinbase: " + blockHeader.getHash());
        final Block block = DiskUtil.loadBlock(blockHeader.getHash(), defaultScenarioDirectory);

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

    protected void _sendBlocks(final List<BlockHeader> scenarioBlocks, final Long runningBlockHeight, final File blockDirectory, final Boolean shouldWait) {
        int i = 1;
        for (final BlockHeader blockHeader : scenarioBlocks) {
            final Sha256Hash blockHash = blockHeader.getHash();
            final Block block = DiskUtil.loadBlock(blockHash, blockDirectory);
            final Long blockHeight = (runningBlockHeight + i);
            _sendBlock(block, blockHeight, blockDirectory, shouldWait);
            i += 1;
        }
    }

    public Main() {
        if (IS_BITCOIN_VERDE_NODE) {
            _rpcAddress = new BitcoinNodeRpcAddress("localhost", 18334);
            _rpcCredentials = null;
        }
        else {
            // bitcoin.conf:
            //  server=1
            //  txindex=1
            //  zmqpubhashblock=tcp://0.0.0.0:8433
            //  rpcauth=root:b971ece882a77bff1a4803c5e7b418fc$a242915ce44f887e8c28b42cfdd87592d1abffa47084e4fb7718dc982c80636a

            _rpcAddress = new BitcoinNodeRpcAddress("localhost", 8332);
            _rpcCredentials = new RpcCredentials("root", "luaDH5Orq8oTJUJhxz2LP4OV1qlCu62OBl26xDhz8Lk=");
        }
    }

    public void run() {
        final int coinbaseMaturityBlockCount = 100;

        final MutableList<BlockHeader> blockHeaders = new MutableList<>(0);

        final File blocksBaseDirectory = new File("data/blocks");
        final File defaultScenarioDirectory = new File(blocksBaseDirectory, "default");
        final int defaultScenarioBlockCount = (1 + 144 + 100 + 10 + 5 + 2 + 5 + 10); // 277

        if (USE_P2P_BROADCAST) {
            _threadPool = new CachedThreadPool(12, 300000L);
            _threadPool.start();

            final LocalNodeFeatures nodeFeatures = new LocalNodeFeatures() {
                @Override
                public NodeFeatures getNodeFeatures() {
                    final NodeFeatures nodeFeatures = new NodeFeatures();
                    nodeFeatures.enableFeature(NodeFeatures.Feature.BITCOIN_CASH_ENABLED);
                    // nodeFeatures.enableFeature(NodeFeatures.Feature.XTHIN_PROTOCOL_ENABLED);
                    // nodeFeatures.enableFeature(NodeFeatures.Feature.BLOOM_CONNECTIONS_ENABLED);

                    nodeFeatures.enableFeature(NodeFeatures.Feature.BLOCKCHAIN_ENABLED);
                    nodeFeatures.enableFeature(NodeFeatures.Feature.MINIMUM_OF_TWO_DAYS_BLOCKCHAIN_ENABLED);

                    return nodeFeatures;
                }
            };

            final BitcoinNode.RequestDataHandler requestDataHandler = new BitcoinNode.RequestDataHandler() {
                @Override
                public void run(final BitcoinNode bitcoinNode, final List<InventoryItem> dataHashes) {
                    for (final InventoryItem inventoryItem : dataHashes) {
                        final InventoryItemType itemType = inventoryItem.getItemType();
                        if (itemType == InventoryItemType.BLOCK) {
                            Logger.debug("Node Requested: " + inventoryItem);
                            final Sha256Hash blockHash = inventoryItem.getItemHash();
                            final Block block = Util.coalesce(_initBlocks.get(blockHash), DiskUtil.loadBlock(blockHash, defaultScenarioDirectory));
                            if (block == null) { continue; }

                            bitcoinNode.transmitBlock(block);
                            Logger.debug("Served: " + blockHash);

                            synchronized (_servedBlocks) {
                                _servedBlocks.add(blockHash);
                                _servedBlocks.notifyAll();
                            }
                        }
                        else if (itemType == InventoryItemType.TRANSACTION) {
                            final Sha256Hash transactionHash = inventoryItem.getItemHash();
                            final Transaction transaction = _transactionsBeingServed.get(transactionHash);
                            if (transaction != null) {
                                bitcoinNode.transmitTransaction(transaction);
                                // Logger.debug("Served: " + transactionHash);
                            }
                        }
                    }
                }
            };

            final BitcoinNode.RequestBlockHeadersHandler requestBlockHeadersHandler = new BitcoinNode.RequestBlockHeadersHandler() {
                @Override
                public void run(final BitcoinNode bitcoinNode, final List<Sha256Hash> blockHashes, final Sha256Hash desiredBlockHash) {
                    final Sha256Hash currentBlock = _currentBlockHash;
                    if (currentBlock != null) {
                        _bitcoinNode.transmitBlockFinder(new ImmutableList<>(currentBlock));
                    }
                    else {
                        _bitcoinNode.transmitBlockFinder(new ImmutableList<>(_genesisBlockHash));
                    }
                }
            };

            final Pin pin = new Pin();

            final Node.DisconnectedCallback disconnectedCallback = new Node.DisconnectedCallback() {
                @Override
                public void onNodeDisconnected() {
                    final Long blockHeight = (_bitcoinNode != null ? _bitcoinNode.getBlockHeight() : null);
                    _bitcoinNode = new BitcoinNode("localhost", 8333, _threadPool, nodeFeatures);
                    Logger.debug("Connecting to Node: " + _bitcoinNode);
                    if (blockHeight != null) {
                        _bitcoinNode.setBlockHeight(blockHeight);
                    }
                    _bitcoinNode.setRequestDataHandler(requestDataHandler);
                    _bitcoinNode.setRequestBlockHeadersHandler(requestBlockHeadersHandler);
                    _bitcoinNode.setDisconnectedCallback(this);
                    _bitcoinNode.setHandshakeCompleteCallback(new Node.HandshakeCompleteCallback() {
                        @Override
                        public void onHandshakeComplete() {
                            Logger.debug("Handshake complete.");
                            pin.release();
                        }
                    });
                    _bitcoinNode.enableNewBlockViaHeaders();
                    _bitcoinNode.connect();
                    _bitcoinNode.handshake();
                }
            };

            disconnectedCallback.onNodeDisconnected(); // Init _bitcoinNode...
            pin.waitForRelease();
        }

        final Tuple<MutableList<BlockHeader>, MutableList<Block>> tuple = _loadInitBlocks(blocksBaseDirectory);
        final List<BlockHeader> initBlockHeaders = tuple.first;
        for (final Block block : tuple.second) {
            final Sha256Hash blockHash = block.getHash();
            _initBlocks.put(blockHash, block);
        }
        blockHeaders.addAll(initBlockHeaders);

        final File manifestFile = new File(defaultScenarioDirectory, "manifest.json");
        final Json blocksManifestJson;
        final Integer manifestBlockCount;
        if (manifestFile.exists()) {
            blocksManifestJson = Json.parse(StringUtil.bytesToString(IoUtil.getFileContents(manifestFile)));
            manifestBlockCount = blocksManifestJson.length();
        }
        else {
            blocksManifestJson = null;
            manifestBlockCount = 0;
        }

        final int initBlockCount = initBlockHeaders.getCount();
        final boolean allBlocksAreMined = ((manifestBlockCount + initBlockCount) >= defaultScenarioBlockCount);
        if (allBlocksAreMined) {
            if (USE_P2P_BROADCAST) {
                _bitcoinNode.transmitBlockHeaders(blockHeaders);

                for (int i = 0; i < manifestBlockCount; ++i) {
                    final Sha256Hash blockHash = Sha256Hash.fromHexString(blocksManifestJson.getString(i));

                    final Long blockHeight = (long) (initBlockCount + i);
                    final Block block = DiskUtil.loadBlock(blockHash, defaultScenarioDirectory);
                    final Boolean shouldWait = (i > 2);
                    _sendBlock(block, blockHeight, defaultScenarioDirectory, shouldWait);
                }

                while (true) {
                    try {
                        Thread.sleep(1000L);
                    }
                    catch (final Exception exception) {
                        break;
                    }
                }
            }
            else {
                for (int i = 0; i < manifestBlockCount; ++i) {
                    final Sha256Hash blockHash = Sha256Hash.fromHexString(blocksManifestJson.getString(i));

                    final Long blockHeight = (long) (initBlockCount + i);
                    final Block block = DiskUtil.loadBlock(blockHash, defaultScenarioDirectory);
                    _sendBlock(block, blockHeight, defaultScenarioDirectory, true);

                    final BlockHeader blockHeader = new ImmutableBlockHeader(block);
                    blockHeaders.add(blockHeader);
                }
            }
            return;
        }

        // All blocks are not mined...
        final PrivateKeyGenerator privateKeyGenerator = new PrivateKeyGenerator() {
            @Override
            public PrivateKey getCoinbasePrivateKey(final Long blockHeight) {
                return Main.derivePrivateKey(blockHeight, 50L * Transaction.SATOSHIS_PER_BITCOIN);
            }
        };

        final Json newManifestJson = new Json(true);

        // Copy the existing blocks to the new manifest file...
        if (blocksManifestJson != null) {
            for (int i = 0; i < manifestBlockCount; ++i) {
                final String blockHashString = blocksManifestJson.getString(i);
                newManifestJson.add(blockHashString);
            }
        }
        long runningBlockHeight = (blockHeaders.getCount() - 1L);

        final Long firstSpendableCoinbaseBlockHeight = (long) blockHeaders.getCount();
        Logger.info("Generating spendable coinbase blocks: " + runningBlockHeight);
        // final long firstScenarioBlockHeight = blockHeaders.getCount();
        {
            final int targetNewBlockCount = coinbaseMaturityBlockCount;
            final MutableList<BlockHeader> scenarioBlocks = _loadBlocksFromManifest(blocksManifestJson, runningBlockHeight, targetNewBlockCount, defaultScenarioDirectory);
            _sendBlocks(scenarioBlocks, runningBlockHeight, defaultScenarioDirectory, false);
            final int newBlockCount = (targetNewBlockCount - scenarioBlocks.getCount());
            scenarioBlocks.addAll(_generateBlocks(privateKeyGenerator, newBlockCount, defaultScenarioDirectory, blockHeaders, _calculateTimestamp(blockHeaders)));
            blockHeaders.addAll(scenarioBlocks);
            for (final BlockHeader blockHeader : scenarioBlocks) {
                final Sha256Hash blockHash = blockHeader.getHash();
                newManifestJson.add(blockHash);
            }
            runningBlockHeight += scenarioBlocks.getCount();
        }

        Logger.info("Generating fan-out blocks: " + runningBlockHeight);
        final Long firstFanOutBlockHeight = (long) blockHeaders.getCount();
        {
            final int targetNewBlockCount = 10;
            final MutableList<BlockHeader> fanOutBlocks = _loadBlocksFromManifest(blocksManifestJson, runningBlockHeight, targetNewBlockCount, defaultScenarioDirectory);
            _sendBlocks(fanOutBlocks, runningBlockHeight, defaultScenarioDirectory, false);
            final int newBlockCount = (targetNewBlockCount - fanOutBlocks.getCount());
            fanOutBlocks.addAll(_generateBlocks(privateKeyGenerator, newBlockCount, defaultScenarioDirectory, blockHeaders, new TransactionGenerator() {
                @Override
                public List<Transaction> getTransactions(final Long blockHeight, final List<BlockHeader> createdBlocks) {
                    final long coinbaseToSpendBlockHeight = (blockHeight - coinbaseMaturityBlockCount); // preFanOutBlock...
                    final BlockHeader blockHeader = blockHeaders.get((int) coinbaseToSpendBlockHeight);
                    final Block block = DiskUtil.loadBlock(blockHeader.getHash(), defaultScenarioDirectory);

                    final Transaction transactionToSplit = block.getCoinbaseTransaction();
                    final PrivateKey coinbasePrivateKey = privateKeyGenerator.getCoinbasePrivateKey(coinbaseToSpendBlockHeight);

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
                        final MutableList<Transaction> transactions = GenerationUtil.createFanOutTransactions(transaction, transactionPrivateKey, blockHeight);

                        _writeTransactionGenerationOrder(transaction, transactions, defaultScenarioDirectory, blockHeight);

                        transactions.add(transaction);
                        return transactions;
                    }
                    catch (final Exception exception) {
                        Logger.warn(exception);
                        return null;
                    }
                }
            }, _calculateTimestamp(blockHeaders)));
            blockHeaders.addAll(fanOutBlocks);
            for (final BlockHeader blockHeader : fanOutBlocks) {
                final Sha256Hash blockHash = blockHeader.getHash();
                newManifestJson.add(blockHash);
            }
            runningBlockHeight += fanOutBlocks.getCount();
        }

        Logger.info("Generating quasi-steady-state blocks: " + runningBlockHeight);
        final Long firstQuasiSteadyStateBlockHeight = (long) blockHeaders.getCount();
        {
            final int targetNewBlockCount = 5;
            final MutableList<BlockHeader> quasiSteadyStateBlocks = _loadBlocksFromManifest(blocksManifestJson, runningBlockHeight, targetNewBlockCount, defaultScenarioDirectory);
            _sendBlocks(quasiSteadyStateBlocks, runningBlockHeight, defaultScenarioDirectory, false);
            final int newBlockCount = (targetNewBlockCount - quasiSteadyStateBlocks.getCount());
            quasiSteadyStateBlocks.addAll(_generateBlocks(privateKeyGenerator, newBlockCount, defaultScenarioDirectory, blockHeaders, new TransactionGenerator() {
                @Override
                public List<Transaction> getTransactions(final Long blockHeight, final List<BlockHeader> createdBlocks) {
                    final MutableList<TransactionWithBlockHeight> transactionsToSpend = new MutableList<>();
                    {
                        final StringBuilder stringBuilder = new StringBuilder("blockHeight=" + blockHeight);
                        // Spending only first half of each 10 fan-out blocks, over 5 quasi-steady-state blocks, means each quasi-steady state block spends 1/5 of 1/2 of each fan-out block.
                        final int quasiSteadyStateBlockIndex = (int) (blockHeight - firstQuasiSteadyStateBlockHeight);
                        stringBuilder.append(" quasiSteadyStateBlockIndex=" + quasiSteadyStateBlockIndex);
                        for (int i = 0; i < 10; ++i) {
                            stringBuilder.append(" (");
                            final long blockHeightToSpend = (firstFanOutBlockHeight + i); // (firstFanOutBlockHeight + steadyStateBlockIndex + i)
                            final Sha256Hash blockHash = Main.getBlockHash(blockHeightToSpend, blockHeaders, createdBlocks);
                            final Block blockToSpend = DiskUtil.loadBlock(blockHash, defaultScenarioDirectory);
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
                        _writeTransactionGenerationOrder(null, transactions, defaultScenarioDirectory, blockHeight);
                        return transactions;
                    }
                    catch (final Exception exception) {
                        Logger.warn(exception);
                        return null;
                    }
                }
            }, _calculateTimestamp(blockHeaders)));
            blockHeaders.addAll(quasiSteadyStateBlocks);
            for (final BlockHeader blockHeader : quasiSteadyStateBlocks) {
                final Sha256Hash blockHash = blockHeader.getHash();
                newManifestJson.add(blockHash);
            }
            runningBlockHeight += quasiSteadyStateBlocks.getCount();
        }

        Logger.info("Generating fan-in blocks: " + runningBlockHeight);
        final Long firstFanInBlockHeight = (long) blockHeaders.getCount();
        {
            final int targetNewBlockCount = 2;
            final MutableList<BlockHeader> fanInBlocks = _loadBlocksFromManifest(blocksManifestJson, runningBlockHeight, targetNewBlockCount, defaultScenarioDirectory);
            _sendBlocks(fanInBlocks, runningBlockHeight, defaultScenarioDirectory, false);
            final int newBlockCount = (targetNewBlockCount - fanInBlocks.getCount());
            fanInBlocks.addAll(_generateBlocks(privateKeyGenerator, newBlockCount, defaultScenarioDirectory, blockHeaders, new TransactionGenerator() {
                @Override
                public List<Transaction> getTransactions(final Long blockHeight, final List<BlockHeader> createdBlocks) {
                    final MutableList<TransactionWithBlockHeight> transactionsToSpend;
                    {
                        final long blockHeightToSpend = (firstQuasiSteadyStateBlockHeight + (blockHeight - firstFanInBlockHeight)); // spend the quasi-steady-state blocks txns in-order...
                        final Sha256Hash blockHash = blockHeaders.get((int) blockHeightToSpend).getHash();
                        final Block blockToSpend = DiskUtil.loadBlock(blockHash, defaultScenarioDirectory);
                        final int transactionCount = blockToSpend.getTransactionCount();
                        transactionsToSpend = new MutableList<>(transactionCount - 1);

                        final List<Transaction> transactions = blockToSpend.getTransactions();
                        for (int i = 1; i < transactionCount; ++i) {
                            final Transaction transaction = transactions.get(i);
                            transactionsToSpend.add(new TransactionWithBlockHeight(transaction, blockHeightToSpend));
                        }
                    }

                    try {
                        final List<Transaction> transactions = GenerationUtil.createFanInTransactions(transactionsToSpend, blockHeight);
                        _writeTransactionGenerationOrder(null, transactions, defaultScenarioDirectory, blockHeight);
                        return transactions;
                    }
                    catch (final Exception exception) {
                        Logger.warn(exception);
                        return null;
                    }
                }
            }, _calculateTimestamp(blockHeaders)));
            blockHeaders.addAll(fanInBlocks);
            for (final BlockHeader blockHeader : fanInBlocks) {
                final Sha256Hash blockHash = blockHeader.getHash();
                newManifestJson.add(blockHash);
            }
            runningBlockHeight += fanInBlocks.getCount();
        }

        Logger.info("Generating 2nd quasi-steady-state blocks: " + runningBlockHeight);
        final long firstQuasiSteadyStateBlockHeightRoundTwo = blockHeaders.getCount();
        final MutableList<BlockHeader> quasiSteadyStateBlocksRoundTwo;
        {
            final int targetNewBlockCount = 5;
            quasiSteadyStateBlocksRoundTwo = _loadBlocksFromManifest(blocksManifestJson, runningBlockHeight, targetNewBlockCount, defaultScenarioDirectory);
            Logger.debug("Loaded " + quasiSteadyStateBlocksRoundTwo.getCount() + " from manifest...");
            _sendBlocks(quasiSteadyStateBlocksRoundTwo, runningBlockHeight, defaultScenarioDirectory, false);
            final int newBlockCount = (targetNewBlockCount - quasiSteadyStateBlocksRoundTwo.getCount());
            quasiSteadyStateBlocksRoundTwo.addAll(_generateBlocks(privateKeyGenerator, newBlockCount, defaultScenarioDirectory, blockHeaders, new TransactionGenerator() {
                @Override
                public List<Transaction> getTransactions(final Long blockHeight, final List<BlockHeader> createdBlocks) {
                    final MutableList<TransactionWithBlockHeight> transactionsToSpend = new MutableList<>();
                    {
                        final int steadyStateBlockIndex = (int) (blockHeight - firstQuasiSteadyStateBlockHeightRoundTwo); // the Nth of 5 steady state blocks...

                        // Spend the respective fan-in block...
                        {
                            final long blockHeightToSpend = (firstFanInBlockHeight + steadyStateBlockIndex);
                            final Sha256Hash blockHash = Main.getBlockHash(blockHeightToSpend, blockHeaders, createdBlocks);
                            final Block blockToSpend = DiskUtil.loadBlock(blockHash, defaultScenarioDirectory);
                            final List<Transaction> transactions = blockToSpend.getTransactions();
                            for (int i = 1; i < transactions.getCount(); ++i) {
                                final Transaction transaction = transactions.get(i);
                                transactionsToSpend.add(new TransactionWithBlockHeight(transaction, blockHeightToSpend));
                            }
                        }

                        // And spend only the second half of each 10 fan-out blocks, over 5 steady-state blocks, means each steady state block spends 1/5 of 1/2 of each fan-out block.
                        for (int i = 0; i < 10; ++i) {
                            final long blockHeightToSpend = (firstFanOutBlockHeight + steadyStateBlockIndex + i);
                            final Sha256Hash blockHash = Main.getBlockHash(blockHeightToSpend, blockHeaders, createdBlocks);
                            final Block blockToSpend = DiskUtil.loadBlock(blockHash, defaultScenarioDirectory);

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

                    try {
                        final List<Transaction> transactions = GenerationUtil.createQuasiSteadyStateTransactions(transactionsToSpend, blockHeight);
                        _writeTransactionGenerationOrder(null, transactions, defaultScenarioDirectory, blockHeight);
                        return transactions;
                    }
                    catch (final Exception exception) {
                        Logger.warn(exception);
                        return null;
                    }
                }
            }, _calculateTimestamp(blockHeaders)));
            blockHeaders.addAll(quasiSteadyStateBlocksRoundTwo);
            for (final BlockHeader blockHeader : quasiSteadyStateBlocksRoundTwo) {
                final Sha256Hash blockHash = blockHeader.getHash();
                newManifestJson.add(blockHash);
            }
            runningBlockHeight += quasiSteadyStateBlocksRoundTwo.getCount();
        }

        Logger.info("Generating steady-state blocks: " + runningBlockHeight);
        // Spends all UTXOs from block# 145, excluding coinbases.
        // Spends coinbases from 156-160 (inclusive).
        final Long firstSteadyStateBlockHeight = (long) blockHeaders.getCount();
        {
            final HashMap<Sha256Hash, HashMap<Integer, TestUtxo>> utxoMap = new HashMap<>();
            {
                long blockHeight = firstSpendableCoinbaseBlockHeight;
                while (blockHeight < blockHeaders.getCount()) {
                    // Logger.debug("Loading Block Height: " + blockHeight);
                    final BlockHeader blockHeader = blockHeaders.get((int) blockHeight);
                    final Block block = DiskUtil.loadBlock(blockHeader.getHash(), defaultScenarioDirectory);
                    // Logger.debug("Block Hash: " + blockHeader.getHash() + " " + (block != null ? "block" : null));

                    boolean isCoinbase = true;
                    for (final Transaction transaction : block.getTransactions()) {
                        if (isCoinbase) {
                            // final boolean isSpendableCoinbase = ((firstSpendableCoinbaseBlockHeight - blockHeight) >= 100L);
                            isCoinbase = false;

                            // if (! isSpendableCoinbase) { continue; }
                            continue;
                        }

                        final List<TransactionOutput> transactionOutputs = transaction.getTransactionOutputs();
                        final int outputCount = transactionOutputs.getCount();
                        for (int outputIndex = 0; outputIndex < outputCount; ++outputIndex) {
                            final TestUtxo testUtxo = new TestUtxo(transaction, outputIndex, blockHeight);
                            final Sha256Hash prevoutHash = transaction.getHash();
                            if (! utxoMap.containsKey(prevoutHash)) {
                                utxoMap.put(prevoutHash, new HashMap<>(1));
                            }

                            final HashMap<Integer, TestUtxo> testUtxos = utxoMap.get(prevoutHash);
                            testUtxos.put(outputIndex, testUtxo);
                        }
                    }

                    isCoinbase = true;
                    for (final Transaction transaction : block.getTransactions()) {
                        if (isCoinbase) {
                            isCoinbase = false;
                            continue;
                        }

                        for (final TransactionInput transactionInput : transaction.getTransactionInputs()) {
                            final Sha256Hash prevoutHash = transactionInput.getPreviousOutputTransactionHash();
                            if (! utxoMap.containsKey(prevoutHash)) { continue; }

                            final Integer prevoutIndex = transactionInput.getPreviousOutputIndex();
                            final HashMap<Integer, TestUtxo> utxoSet = utxoMap.get(prevoutHash);
                            utxoSet.remove(prevoutIndex);
                        }
                    }

                    blockHeight += 1L;
                }
            }

            final MutableList<TestUtxo> availableUtxos = new MutableList<>(0);
            for (final HashMap<Integer, TestUtxo> map : utxoMap.values()) {
                availableUtxos.addAll(map.values());
            }
            Logger.debug("availableUtxos.count=" + availableUtxos.getCount());

            // Arbitrarily order/mix the UTXOs...
            final Comparator<TestUtxo> testUtxoComparator = new Comparator<TestUtxo>() {
                @Override
                public int compare(final TestUtxo testUtxo0, final TestUtxo testUtxo1) {
                    final Transaction transaction0 = testUtxo0.getTransaction();
                    final Sha256Hash transactionHash0 = transaction0.getHash();
                    final long value0 = ByteUtil.byteToLong(transactionHash0.getByte(Sha256Hash.BYTE_COUNT - 1)) + (testUtxo0.getOutputIndex() * testUtxo0.getAmount());

                    final Transaction transaction1 = testUtxo1.getTransaction();
                    final Sha256Hash transactionHash1 = transaction1.getHash();
                    final long value1 = ByteUtil.byteToLong(transactionHash1.getByte(Sha256Hash.BYTE_COUNT - 1)) + (testUtxo1.getOutputIndex() * testUtxo1.getAmount());

                    return Long.compare(value0, value1);
                }
            };
            availableUtxos.sort(testUtxoComparator);

            final int targetNewBlockCount = 10;
            final MutableList<BlockHeader> steadyStateBlocks = _loadBlocksFromManifest(blocksManifestJson, runningBlockHeight, targetNewBlockCount, defaultScenarioDirectory);
            _sendBlocks(steadyStateBlocks, runningBlockHeight, defaultScenarioDirectory, false);
            final int newBlockCount = (targetNewBlockCount - steadyStateBlocks.getCount());
            steadyStateBlocks.addAll(_generateBlocks(privateKeyGenerator, newBlockCount, defaultScenarioDirectory, blockHeaders, new TransactionGenerator() {
                @Override
                public List<Transaction> getTransactions(final Long blockHeight, final List<BlockHeader> createdBlocks) {
                    final int blockOffset = (int) (blockHeight - firstSteadyStateBlockHeight);
                    final long spendBlockHeight = (156L + blockOffset);
                    Logger.debug("Spending Block #" + spendBlockHeight + "'s coinbase.");
                    final Transaction additionalTransactionFromPreviousCoinbase = _splitCoinbaseTransaction(blockHeight, spendBlockHeight, blockHeaders, defaultScenarioDirectory, privateKeyGenerator);
                    { // Add the coinbase UTXOs to the pool.
                        final List<TransactionOutput> transactionOutputs = additionalTransactionFromPreviousCoinbase.getTransactionOutputs();
                        for (int outputIndex = 0; outputIndex < transactionOutputs.getCount(); outputIndex += 1) {
                            availableUtxos.add(new TestUtxo(additionalTransactionFromPreviousCoinbase, outputIndex, blockHeight));
                        }

                        // Arbitrarily re-order/mix the UTXOs...
                        availableUtxos.sort(testUtxoComparator);
                    }

                    Logger.debug("availableUtxos.count=" + availableUtxos.getCount());

                    try {
                        final List<Transaction> generatedTransactions = GenerationUtil.createCashTransactions(additionalTransactionFromPreviousCoinbase, availableUtxos, blockHeight);
                        _writeTransactionGenerationOrder(additionalTransactionFromPreviousCoinbase, generatedTransactions, defaultScenarioDirectory, blockHeight);

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
            }, _calculateTimestamp(blockHeaders)));
            blockHeaders.addAll(steadyStateBlocks);
            for (final BlockHeader blockHeader : steadyStateBlocks) {
                final Sha256Hash blockHash = blockHeader.getHash();
                newManifestJson.add(blockHash);
            }
            runningBlockHeight += steadyStateBlocks.getCount();
        }

        IoUtil.putFileContents(manifestFile, StringUtil.stringToBytes(newManifestJson.toString()));
    }

    @Override
    public void close() {
        if (_bitcoinNode != null) {
            _bitcoinNode.disconnect();
        }

        if (_threadPool != null) {
            _threadPool.stop();
        }
    }
}