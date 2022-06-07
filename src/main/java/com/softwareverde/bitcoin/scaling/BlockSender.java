package com.softwareverde.bitcoin.scaling;

import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.header.BlockHeaderWithTransactionCount;
import com.softwareverde.bitcoin.block.header.ImmutableBlockHeader;
import com.softwareverde.bitcoin.block.header.ImmutableBlockHeaderWithTransactionCount;
import com.softwareverde.bitcoin.rpc.BitcoinMiningRpcConnector;
import com.softwareverde.bitcoin.rpc.BitcoinNodeRpcAddress;
import com.softwareverde.bitcoin.rpc.BitcoinVerdeRpcConnector;
import com.softwareverde.bitcoin.rpc.RpcCredentials;
import com.softwareverde.bitcoin.rpc.core.BitcoinCoreRpcConnector;
import com.softwareverde.bitcoin.scaling.rpc.BitcoinCoreRpcConnector2;
import com.softwareverde.bitcoin.scaling.rpc.TransactionRpcConnector;
import com.softwareverde.bitcoin.scaling.rpc.VerdeTransactionRpcConnector;
import com.softwareverde.bitcoin.server.main.BitcoinConstants;
import com.softwareverde.bitcoin.server.message.type.node.feature.LocalNodeFeatures;
import com.softwareverde.bitcoin.server.message.type.node.feature.NodeFeatures;
import com.softwareverde.bitcoin.server.message.type.query.response.hash.InventoryItem;
import com.softwareverde.bitcoin.server.message.type.query.response.hash.InventoryItemType;
import com.softwareverde.bitcoin.server.node.BitcoinNode;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.concurrent.ConcurrentHashSet;
import com.softwareverde.concurrent.Pin;
import com.softwareverde.concurrent.threadpool.CachedThreadPool;
import com.softwareverde.constable.bytearray.MutableByteArray;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.immutable.ImmutableList;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.cryptography.hash.sha256.MutableSha256Hash;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.json.Json;
import com.softwareverde.logging.Logger;
import com.softwareverde.network.p2p.node.Node;
import com.softwareverde.util.Util;
import com.softwareverde.util.timer.NanoTimer;
import com.softwareverde.util.type.time.SystemTime;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class BlockSender {
    protected final Sha256Hash _genesisBlockHash = Sha256Hash.fromHexString(BitcoinConstants.getGenesisBlockHash());
    protected CachedThreadPool _threadPool;
    protected BitcoinNode _bitcoinNode;
    protected File _scenarioDirectory;
    protected boolean _shouldUseP2PBroadcast;
    protected final ConcurrentHashSet<Sha256Hash> _servedBlocks = new ConcurrentHashSet<>();
    protected Sha256Hash _currentBlockHash;
    protected ConcurrentHashMap<Sha256Hash, Transaction> _transactionsBeingServed = new ConcurrentHashMap<>();

    protected boolean _isEnabled = true;
    protected float _preRelayPercent = 0F;

    protected final ConcurrentHashMap<Sha256Hash, Block> _initBlocks;
    protected final BitcoinNodeRpcAddress _rpcAddress;
    protected final RpcCredentials _rpcCredentials;
    protected final boolean _isBitcoinVerdeNode;

    public BlockSender(final File scenarioDirectory, final ConcurrentHashMap<Sha256Hash, Block> initBlocks, final boolean shouldUseP2PBroadcast, final BitcoinNodeRpcAddress rpcAddress, final RpcCredentials rpcCredentials, final boolean isBitcoinVerdeNode) {
        _scenarioDirectory = scenarioDirectory;
        _initBlocks = initBlocks;
        _shouldUseP2PBroadcast = shouldUseP2PBroadcast;
        _rpcAddress = rpcAddress;
        _rpcCredentials = rpcCredentials;
        _isBitcoinVerdeNode = isBitcoinVerdeNode;
        if (shouldUseP2PBroadcast) {
            _setUpBitcoinPeer(scenarioDirectory);
        }
    }

    public void setEnabled(boolean enabled) {
        _isEnabled = enabled;
    }

    public void setPreRelayPercent(float preRelayPercent) {
        _preRelayPercent = preRelayPercent;
    }

    public void sendBlockHeader(final BlockHeader block, final Integer transactionCount) {
        final Sha256Hash blockHash = block.getHash();
        _currentBlockHash = blockHash; // TODO: this is just tracking the current tip block, should this be managed outside of this class?

        if (!_isEnabled) {
            Logger.debug("Skipped Header: " + blockHash);
            return;
        }

        final BlockHeaderWithTransactionCount blockHeader = new ImmutableBlockHeaderWithTransactionCount(block, transactionCount);
        _bitcoinNode.transmitBlockHeader(blockHeader);

        Logger.debug("Sent Header: " + blockHash);
    }

    public void sendBlock(final Block block, final Long blockHeight, final File scenarioDirectory, final Boolean shouldWait) {
        final Sha256Hash blockHash = block.getHash();
        // _currentBlockHash = blockHash;

        if (!_isEnabled) {
            Logger.debug("Skipped Sending Of: " + blockHash);
            return;
        }

        if ( (blockHeight != null) && (_preRelayPercent > 0F) ) {
            final File directory = new File(scenarioDirectory, "mempool");
            final File file = new File(directory, blockHeight + ".sha");
            if (file.exists()) {
                final int blockTransactionCount = block.getTransactionCount();
                final int transactionCount = (int) ((blockTransactionCount - 1) * _preRelayPercent);
                final int seconds = (10 * 60);
                final int batchSizePerSecond = (transactionCount / seconds);

                int sentTransactionsCount = 0;
                if (_shouldUseP2PBroadcast) {
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

                    try (final TransactionRpcConnector transactionRpcConnector = (_isBitcoinVerdeNode ? new VerdeTransactionRpcConnector(_rpcAddress) : new BitcoinCoreRpcConnector2(_rpcAddress, _rpcCredentials))) {
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
        else if (shouldWait) {
            final SystemTime systemTime = new SystemTime();
            final long diffTime = (block.getTimestamp() - systemTime.getCurrentTimeInSeconds());
            if (diffTime > 0L) {
                try {
                    Thread.sleep(diffTime * 1000L);
                }
                catch (final Exception exception) {
                    // Nothing.
                }
            }
        }

        Logger.debug("Sending Full Block: " + blockHash + " " + blockHeight);

        if (_shouldUseP2PBroadcast) {
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
            try (final BitcoinMiningRpcConnector bitcoinRpcConnector = (_isBitcoinVerdeNode ? new BitcoinVerdeRpcConnector(_rpcAddress, _rpcCredentials) : new BitcoinCoreRpcConnector(_rpcAddress, _rpcCredentials))) {
                bitcoinRpcConnector.submitBlock(block);
            }
        }

        Logger.debug("Sent: " + blockHash);
    }

    public void sendBlocks(final List<BlockHeader> scenarioBlocks, final Long runningBlockHeight, final File blockDirectory, final Boolean shouldWait) {
        int i = 1;
        for (final BlockHeader blockHeader : scenarioBlocks) {
            final Sha256Hash blockHash = blockHeader.getHash();
            final Block block = DiskUtil.loadBlock(blockHash, blockDirectory);
            final Long blockHeight = (runningBlockHeight + i);
            this.sendBlock(block, blockHeight, blockDirectory, shouldWait);
            i += 1;
        }
    }

    public void transmitTestBlocks(MutableList<BlockHeader> blockHeaders, final int initBlockCount, final Json blocksManifestJson, final int manifestBlockCount, final Json reorgBlocksManifestJson, final File scenarioDirectory) {
        if (_shouldUseP2PBroadcast) {
            _bitcoinNode.transmitBlockHeaders(blockHeaders);
            for (int i = 0; i < manifestBlockCount; ++i) {
                final Sha256Hash blockHash = Sha256Hash.fromHexString(blocksManifestJson.getString(i));

                final Long blockHeight = (long) (initBlockCount + i);
                if (reorgBlocksManifestJson.hasKey(blockHeight.toString())) {
                    final Json reorgBlockHashesJson = reorgBlocksManifestJson.get(blockHeight.toString());
                    for (int j = 0; j < reorgBlockHashesJson.length(); ++j) {
                        final Sha256Hash reorgBlockHash = Sha256Hash.fromHexString(reorgBlockHashesJson.getString(j));
                        final Block block = DiskUtil.loadBlock(reorgBlockHash, scenarioDirectory);
                        final Boolean shouldWait = (i > 2);
                        this.sendBlock(block, blockHeight, scenarioDirectory, shouldWait);
                    }

                    final Block block = DiskUtil.loadBlock(blockHash, scenarioDirectory);
                    this.sendBlock(block, blockHeight, scenarioDirectory, false); // Override shouldWait since it was reorged...
                }
                else {
                    final Block block = DiskUtil.loadBlock(blockHash, scenarioDirectory);
                    final Boolean shouldWait = (i > 2);
                    this.sendBlock(block, blockHeight, scenarioDirectory, shouldWait);
                }
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
                final Block block = DiskUtil.loadBlock(blockHash, scenarioDirectory);
                this.sendBlock(block, blockHeight, scenarioDirectory, true);

                final BlockHeader blockHeader = new ImmutableBlockHeader(block);
                blockHeaders.add(blockHeader);
            }
        }
    }

    public void close() {
        if (_bitcoinNode != null) {
            _bitcoinNode.disconnect();
        }

        if (_threadPool != null) {
            _threadPool.stop();
        }
    }

    protected void _setUpBitcoinPeer(final File defaultScenarioDirectory) {
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
}