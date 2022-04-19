package com.softwareverde.bitcoin.scaling;

import com.softwareverde.bitcoin.CoreInflater;
import com.softwareverde.bitcoin.address.Address;
import com.softwareverde.bitcoin.address.AddressInflater;
import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockDeflater;
import com.softwareverde.bitcoin.block.BlockInflater;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.header.ImmutableBlockHeader;
import com.softwareverde.bitcoin.block.header.difficulty.Difficulty;
import com.softwareverde.bitcoin.block.validator.difficulty.DifficultyCalculator;
import com.softwareverde.bitcoin.context.DifficultyCalculatorContext;
import com.softwareverde.bitcoin.inflater.MasterInflater;
import com.softwareverde.bitcoin.rpc.BitcoinMiningRpcConnector;
import com.softwareverde.bitcoin.rpc.BitcoinMiningRpcConnectorFactory;
import com.softwareverde.bitcoin.rpc.BitcoinNodeRpcAddress;
import com.softwareverde.bitcoin.rpc.BitcoinVerdeRpcConnector;
import com.softwareverde.bitcoin.rpc.BlockTemplate;
import com.softwareverde.bitcoin.rpc.MutableBlockTemplate;
import com.softwareverde.bitcoin.rpc.RpcCredentials;
import com.softwareverde.bitcoin.rpc.core.BitcoinCoreRpcConnector;
import com.softwareverde.bitcoin.scaling.generate.rpc.PrivateTestNetBitcoinMiningRpcConnector;
import com.softwareverde.bitcoin.secp256k1.privatekey.PrivateKeyDeflater;
import com.softwareverde.bitcoin.server.database.BatchRunner;
import com.softwareverde.bitcoin.stratum.BitcoinCoreStratumServer;
import com.softwareverde.bitcoin.stratum.StratumServer;
import com.softwareverde.bitcoin.stratum.callback.BlockFoundCallback;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.wallet.PaymentAmount;
import com.softwareverde.bitcoin.wallet.Wallet;
import com.softwareverde.concurrent.threadpool.CachedThreadPool;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.constable.bytearray.MutableByteArray;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.immutable.ImmutableList;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.cryptography.secp256k1.key.PrivateKey;
import com.softwareverde.http.HttpResponse;
import com.softwareverde.http.WebRequest;
import com.softwareverde.json.Json;
import com.softwareverde.logging.LineNumberAnnotatedLog;
import com.softwareverde.logging.LogLevel;
import com.softwareverde.logging.Logger;
import com.softwareverde.util.ByteUtil;
import com.softwareverde.util.Container;
import com.softwareverde.util.IoUtil;
import com.softwareverde.util.Util;
import com.softwareverde.util.timer.MultiTimer;
import com.softwareverde.util.timer.NanoTimer;

import java.io.File;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Main {
    protected interface TransactionGenerator {
        List<Transaction> getTransactions(Long blockHeight);
    }

    public static void main(final String[] commandLineArguments) {
        Logger.setLog(LineNumberAnnotatedLog.getInstance());
        Logger.setLogLevel(LogLevel.DEBUG);
        Logger.setLogLevel("com.softwareverde.util", LogLevel.ERROR);
        Logger.setLogLevel("com.softwareverde.network", LogLevel.INFO);
        Logger.setLogLevel("com.softwareverde.async.lock", LogLevel.WARN);
        Logger.setLogLevel("com.softwareverde.bitcoin.stratum.BitcoinCoreStratumServer", LogLevel.WARN);
        Logger.setLogLevel("com.softwareverde.bitcoin.wallet.Wallet", LogLevel.WARN);

        final Main main = new Main();
        main.run();
    }

    protected Block _loadBlock(final Sha256Hash blockHash, final File blocksDirectory) {
        final BlockInflater blockInflater = new BlockInflater();

        final File blockFile = new File(blocksDirectory, blockHash.toString());
        final ByteArray bytes = ByteArray.wrap(IoUtil.getFileContents(blockFile));
        return blockInflater.fromBytes(bytes);
    }

//    protected PrivateKey _derivePrivateKey(final Long value) {
//        final MutableByteArray bytes = new MutableByteArray(PrivateKey.KEY_BYTE_COUNT);
//        final byte[] valueBytes = ByteUtil.longToBytes(value);
//
//        final int startIndex = (PrivateKey.KEY_BYTE_COUNT - valueBytes.length);
//        bytes.setBytes(startIndex, valueBytes);
//
//        return PrivateKey.fromBytes(bytes);
//    }

    protected static PrivateKey derivePrivateKey(final Long blockHeight, final Long outputValue) {
        final MutableByteArray bytes = new MutableByteArray(PrivateKey.KEY_BYTE_COUNT);
        final byte[] blockHeightBytes = ByteUtil.longToBytes(blockHeight);
        final byte[] outputValueBytes = ByteUtil.longToBytes(outputValue);

        final int startIndex = (PrivateKey.KEY_BYTE_COUNT - blockHeightBytes.length - outputValueBytes.length);
        bytes.setBytes(startIndex, blockHeightBytes);
        bytes.setBytes(startIndex + blockHeightBytes.length, outputValueBytes);

        return PrivateKey.fromBytes(bytes);
    }

    protected MutableList<BlockHeader> _generateBlocks(final PrivateKey privateKey, final Integer blockCount, final File blocksDirectory, final List<BlockHeader> initBlocks) {
        return _generateBlocks(privateKey, blockCount, blocksDirectory, initBlocks, null);
    }

    protected MutableList<BlockHeader> _generateBlocks(final PrivateKey privateKey, final Integer blockCount, final File blocksDirectory, final List<BlockHeader> initBlocks, final TransactionGenerator transactionGenerator) {
        final PrivateTestNetDifficultyCalculatorContext difficultyCalculatorContext = new PrivateTestNetDifficultyCalculatorContext();
        difficultyCalculatorContext.addBlocks(initBlocks);

        final Address address;
        {
            final AddressInflater addressInflater = new AddressInflater();
            address = addressInflater.fromPrivateKey(privateKey, true);
        }

        final Container<BlockTemplate> blockTemplateContainer;
        {
            final int blocksCount = initBlocks.getCount();
            final BlockHeader previousBlock = initBlocks.get(blocksCount - 1);
            final Sha256Hash previousBlockHash = previousBlock.getHash();

            final Long nextBlockHeight = (long) blocksCount;
            final List<Transaction> transactions = ((transactionGenerator != null) ? transactionGenerator.getTransactions(nextBlockHeight) : new MutableList<>(0));
            final BlockTemplate blockTemplate = _createBlockTemplate(previousBlockHash, nextBlockHeight, transactions, difficultyCalculatorContext);
            blockTemplateContainer = new Container<>(blockTemplate);
        }

        final BitcoinMiningRpcConnectorFactory rpcConnectorFactory = new BitcoinMiningRpcConnectorFactory() {
            @Override
            public BitcoinMiningRpcConnector newBitcoinMiningRpcConnector() {
                synchronized (blockTemplateContainer) {
                    return new PrivateTestNetBitcoinMiningRpcConnector(blockTemplateContainer.value);
                }
            }
        };

        final BlockDeflater blockDeflater = new BlockDeflater();
        final MasterInflater masterInflater = new CoreInflater();
        final CachedThreadPool threadPool = new CachedThreadPool(32, 10000L);
        final StratumServer stratumServer = new BitcoinCoreStratumServer(rpcConnectorFactory, 3333, threadPool, masterInflater);

        if (! blocksDirectory.exists()) { blocksDirectory.mkdirs(); }

        final int initBlockCount = initBlocks.getCount();
        final BlockHeader lastInitBlock = initBlocks.get(initBlockCount - 1);
        final MutableList<BlockHeader> blocks = new MutableList<>(blockCount);

        stratumServer.setBlockFoundCallback(new BlockFoundCallback() {
            @Override
            public synchronized void run(final Block block, final String workerName) {
                synchronized (blockTemplateContainer) {
                    final int existingBlockCount;
                    final BlockHeader previousBlock;
                    synchronized (blocks) {
                        existingBlockCount = blocks.getCount();
                        if (existingBlockCount >= blockCount) {
                            blocks.notifyAll();
                            Logger.debug("DONE");
                            return;
                        }

                        Logger.debug("existingBlockCount=" + existingBlockCount);
                        previousBlock = ((existingBlockCount < 1) ? lastInitBlock : blocks.get(existingBlockCount - 1));
                    }
                    Logger.debug("previousBlock=" + previousBlock.getHash());

                    final Sha256Hash blockHash = block.getHash();
                    {
                        final Sha256Hash expectedPreviousBlockHash = previousBlock.getHash();
                        final Sha256Hash previousBlockHash = block.getPreviousBlockHash();

                        if (! Util.areEqual(expectedPreviousBlockHash, previousBlockHash)) {
                            Logger.debug("Rejected: " + blockHash + " " + expectedPreviousBlockHash + " != " + previousBlockHash);
                            return;
                        }
                    }

                    final ByteArray blockBytes = blockDeflater.toBytes(block);

                    final File file = new File(blocksDirectory, blockHash.toString());
                    IoUtil.putFileContents(file, blockBytes);

                    final BlockHeader blockHeader = new ImmutableBlockHeader(block);
                    synchronized (blocks) {
                        blocks.add(blockHeader);
                    }
                    difficultyCalculatorContext.addBlock(blockHeader);

                    final long blockHeight = ( (initBlockCount - 1) + (existingBlockCount + 1) );
                    Logger.info("Height: " + blockHeight + " " + blockHash + " " + file.getPath());

                    _sendBlock(block);

                    if (blocks.getCount() >= blockCount) {
                        synchronized (blocks) {
                            blocks.notifyAll();
                            return;
                        }
                    }

                    final long nextBlockHeight = (blockHeight + 1L);
                    final List<Transaction> transactions = ((transactionGenerator != null) ? transactionGenerator.getTransactions(nextBlockHeight) : new MutableList<>(0));
                    blockTemplateContainer.value = _createBlockTemplate(blockHash, nextBlockHeight, transactions, difficultyCalculatorContext);
                }
            }
        });
        stratumServer.setCoinbaseAddress(address);

        threadPool.start();
        stratumServer.start();

        int createdBlockCount = 0;
        while (createdBlockCount < blockCount) {
            synchronized (blocks) {
                try {
                    blocks.wait();
                }
                catch (final Exception exception) { break; }
                createdBlockCount = blocks.getCount();
            }
        }

        stratumServer.stop();
        threadPool.stop();

        Logger.debug("Generated " + createdBlockCount + " blocks.");

        return blocks;
    }

    protected MutableList<BlockHeader> _loadInitBlocks(final File blocksBaseDirectory) {
//        final Sha256Hash genesisBlockHash = Sha256Hash.fromHexString(BitcoinConstants.MainNet.genesisBlockHash);
//
//        Sha256Hash lastLoadedBlockHash = null;
//        for (int i = 0; i < 3; ++i) {
//            final File blocksDirectory = new File(blocksBaseDirectory, String.valueOf(i));
//            if (! blocksDirectory.isDirectory()) { break; }
//
//            final File[] blocksDirectoryList = blocksDirectory.listFiles();
//            if (blocksDirectoryList == null) { break; }
//
//            final HashMap<Sha256Hash, Block> previousBlockHashMap = new HashMap<>();
//
//            final BlockInflater blockInflater = new BlockInflater();
//            for (final File blockFile : blocksDirectoryList) {
//                final ByteArray blockData = ByteArray.wrap(IoUtil.getFileContents(blockFile));
//                final Block block = blockInflater.fromBytes(blockData);
//                if (block == null) { continue; }
//
//                // If the genesis block hasn't been loaded, add it to the list when it appears.
//                if (lastLoadedBlockHash == null) {
//                    final Sha256Hash blockHash = block.getHash();
//                    if (Util.areEqual(blockHash, genesisBlockHash)) {
//                        _blocks.add(block);
//                        lastLoadedBlockHash = genesisBlockHash;
//
//                        _sendBlock(block);
//                    }
//                }
//
//                final Sha256Hash previousBlockHash = block.getPreviousBlockHash();
//                previousBlockHashMap.put(previousBlockHash, block);
//            }
//
//            while (true) {
//                final Block block = previousBlockHashMap.get(lastLoadedBlockHash);
//                if (block == null) { break; }
//
//                _blocks.add(block);
//                lastLoadedBlockHash = block.getHash();
//
//                _sendBlock(block);
//            }
//        }

        final MutableList<BlockHeader> initBlocks = new MutableList<>();

        final BlockInflater blockInflater = new BlockInflater();
        final Json mainNetBlocks = Json.parse(IoUtil.getResource("/block-hashes.json")); // Genesis through Block #144 (inclusive)
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
            initBlocks.add(blockHeader);

            _sendBlock(block);
        }

        Logger.debug(initBlocks.getCount() + " blocks loaded.");
        Logger.debug("HeadBlock=" + initBlocks.get(initBlocks.getCount() - 1).getHash());

        return initBlocks;
    }

    public Main() { }

    protected BlockTemplate _createBlockTemplate(final Sha256Hash previousBlockHash, final Long blockHeight, final List<Transaction> transactions, final DifficultyCalculatorContext difficultyCalculatorContext) {
        final DifficultyCalculator difficultyCalculator = new DifficultyCalculator(difficultyCalculatorContext);

        final Difficulty difficulty = difficultyCalculator.calculateRequiredDifficulty(blockHeight);

        final Long coinbaseReward = BlockHeader.calculateBlockReward(blockHeight);

        final MutableBlockTemplate blockTemplate = new MutableBlockTemplate();
        blockTemplate.setBlockVersion(Block.VERSION);
        blockTemplate.setDifficulty(difficulty);
        blockTemplate.setPreviousBlockHash(previousBlockHash);
        blockTemplate.setBlockHeight(blockHeight);
        blockTemplate.setCoinbaseAmount(coinbaseReward);

        for (final Transaction transaction : transactions) {
            blockTemplate.addTransaction(transaction, 0L, 0);
        }

        return blockTemplate;
    }

    protected void _sendBlock(final Block block) {
        final boolean isBitcoinVerdeNode = false;
        if (isBitcoinVerdeNode) {
            final BitcoinNodeRpcAddress bitcoinNodeRpcAddress = new BitcoinNodeRpcAddress("localhost", 8334);
            try (final BitcoinVerdeRpcConnector bitcoinRpcConnector = new BitcoinVerdeRpcConnector(bitcoinNodeRpcAddress, null)) {
                bitcoinRpcConnector.submitBlock(block);
            }
        }
        else {
            // bitcoin.conf:
            //  server=1
            //  rpcauth=root:b971ece882a77bff1a4803c5e7b418fc$a242915ce44f887e8c28b42cfdd87592d1abffa47084e4fb7718dc982c80636a

            final BitcoinNodeRpcAddress bitcoinNodeRpcAddress = new BitcoinNodeRpcAddress("localhost", 8332);
            final RpcCredentials rpcCredentials = new RpcCredentials("root", "luaDH5Orq8oTJUJhxz2LP4OV1qlCu62OBl26xDhz8Lk=");
            try (final BitcoinCoreRpcConnector bitcoinRpcConnector = new BitcoinCoreRpcConnector(bitcoinNodeRpcAddress, rpcCredentials)) {
                bitcoinRpcConnector.submitBlock(block);
            }
        }
    }

    protected MutableList<Transaction> _createFanOutTransactions(final Transaction rootTransactionToSpend, final PrivateKey privateKey, final Long blockHeight) throws Exception {
        final int transactionCount = 256000;
        final int outputsPerTransactionCount = 25; // TxSize = 158 + (34 * OutputCount) ~= 1024

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

    public void run() {
        final Integer coinbaseMaturityBlockCount = 100;

        final MutableList<BlockHeader> blockHeaders = new MutableList<>(0);
        Logger.debug("blockHeaders.count = " + blockHeaders.getCount());

        final File blocksBaseDirectory = new File("data/blocks");
        final List<BlockHeader> initBlocks = _loadInitBlocks(blocksBaseDirectory);
        blockHeaders.addAll(initBlocks);
        Logger.debug("blockHeaders.count = " + blockHeaders.getCount());

        final Long firstScenarioBlockHeight = (long) blockHeaders.getCount();
        final PrivateKey privateKey = Main.derivePrivateKey(firstScenarioBlockHeight, 0L);

        final File defaultScenarioDirectory = new File("data/blocks/default");
        final MutableList<BlockHeader> scenarioBlocks = _generateBlocks(privateKey, coinbaseMaturityBlockCount, defaultScenarioDirectory, blockHeaders);
        blockHeaders.addAll(scenarioBlocks);
        Logger.debug("blockHeaders.count = " + blockHeaders.getCount());

        final List<BlockHeader> fanOutBlocks = _generateBlocks(privateKey, 1, defaultScenarioDirectory, blockHeaders, new TransactionGenerator() {
            @Override
            public List<Transaction> getTransactions(final Long blockHeight) {
                final long coinbaseToSpendBlockHeight = (blockHeight - coinbaseMaturityBlockCount); // preFanOutBlock...
                Logger.debug("coinbaseToSpendBlockHeight=" + coinbaseToSpendBlockHeight);
                final BlockHeader blockHeader = blockHeaders.get((int) coinbaseToSpendBlockHeight);
                final Block block = _loadBlock(blockHeader.getHash(), defaultScenarioDirectory);

                final Transaction transactionToSplit = block.getCoinbaseTransaction();

                final Wallet wallet = new Wallet();
                wallet.addPrivateKey(privateKey);
                wallet.addTransaction(transactionToSplit);
                wallet.setSatoshisPerByteFee(0D);

                final Address address = wallet.getReceivingAddress();
                final Long totalOutputValue = transactionToSplit.getTotalOutputValue();

                final int outputCount = 4;
                final Long outputValue = (totalOutputValue / outputCount);
                final MutableList<PaymentAmount> paymentAmounts = new MutableList<>(outputCount);
                for (int i = 0; i < outputCount; ++i) {
                    paymentAmounts.add(new PaymentAmount(address, outputValue));
                }

                final Transaction transaction = wallet.createTransaction(paymentAmounts, address);

                try {
                    final MutableList<Transaction> transactions = _createFanOutTransactions(transaction, privateKey, blockHeight);
                    transactions.add(transaction);
                    return transactions;
                }
                catch (final Exception exception) {
                    Logger.warn(exception);
                    return null;
                }
            }
        });
        blockHeaders.addAll(fanOutBlocks);
        Logger.debug("blockHeaders.count = " + blockHeaders.getCount());

        final PrivateKeyDeflater privateKeyDeflater = new PrivateKeyDeflater();
        final String privateKeyWif = privateKeyDeflater.toWalletImportFormat(privateKey, true);
        System.out.println("Private Key: " + privateKeyWif);
    }
}