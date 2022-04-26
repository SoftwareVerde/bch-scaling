package com.softwareverde.bitcoin.scaling;

import com.softwareverde.bitcoin.CoreInflater;
import com.softwareverde.bitcoin.address.Address;
import com.softwareverde.bitcoin.address.AddressInflater;
import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockDeflater;
import com.softwareverde.bitcoin.block.BlockInflater;
import com.softwareverde.bitcoin.block.MutableBlock;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.header.BlockHeaderInflater;
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
import com.softwareverde.bitcoin.rpc.monitor.Monitor;
import com.softwareverde.bitcoin.scaling.generate.rpc.PrivateTestNetBitcoinMiningRpcConnector;
import com.softwareverde.bitcoin.server.database.BatchRunner;
import com.softwareverde.bitcoin.server.main.BitcoinConstants;
import com.softwareverde.bitcoin.stratum.BitcoinCoreStratumServer;
import com.softwareverde.bitcoin.stratum.StratumServer;
import com.softwareverde.bitcoin.stratum.callback.BlockFoundCallback;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.transaction.TransactionDeflater;
import com.softwareverde.bitcoin.transaction.TransactionInflater;
import com.softwareverde.bitcoin.transaction.coinbase.CoinbaseTransaction;
import com.softwareverde.bitcoin.transaction.output.TransactionOutput;
import com.softwareverde.bitcoin.wallet.PaymentAmount;
import com.softwareverde.bitcoin.wallet.Wallet;
import com.softwareverde.concurrent.threadpool.CachedThreadPool;
import com.softwareverde.concurrent.threadpool.ThreadPool;
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
import com.softwareverde.util.StringUtil;
import com.softwareverde.util.Util;
import com.softwareverde.util.timer.MultiTimer;
import com.softwareverde.util.timer.NanoTimer;
import com.softwareverde.util.type.time.SystemTime;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class Main {
    protected static final Boolean SKIP_MINING = false;

    protected interface TransactionGenerator {
        List<Transaction> getTransactions(Long blockHeight, List<BlockHeader> newBlockHeaders);
    }

    protected interface PrivateKeyGenerator {
        PrivateKey getCoinbasePrivateKey(Long blockHeight);
    }

    public static void main(final String[] commandLineArguments) {
        BitcoinConstants.setBlockMaxByteCount((int) (256L * ByteUtil.Unit.Si.MEGABYTES));

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

    protected BlockHeader _loadBlockHeader(final Sha256Hash blockHash, final File blocksDirectory) {
        final BlockHeaderInflater blockHeaderInflater = new BlockHeaderInflater();

        final String blockHashString = blockHash.toString();
        final File blockFile = new File(blocksDirectory, blockHashString);

        try (final InputStream inputStream = new FileInputStream(blockFile)) {
            final MutableByteArray buffer = new MutableByteArray(BlockHeaderInflater.BLOCK_HEADER_BYTE_COUNT);
            IoUtil.readBytesFromStream(inputStream, buffer.unwrap());

            return blockHeaderInflater.fromBytes(buffer);
        }
        catch (final Exception exception) {
            Logger.debug(exception);
            return null;
        }
    }

    protected static PrivateKey derivePrivateKey(final Long blockHeight, final Long outputValue) {
        final MutableByteArray bytes = new MutableByteArray(PrivateKey.KEY_BYTE_COUNT);
        final byte[] blockHeightBytes = ByteUtil.longToBytes(blockHeight);
        final byte[] outputValueBytes = ByteUtil.longToBytes(outputValue);

        final int startIndex = (PrivateKey.KEY_BYTE_COUNT - blockHeightBytes.length - outputValueBytes.length);
        bytes.setBytes(startIndex, blockHeightBytes);
        bytes.setBytes(startIndex + blockHeightBytes.length, outputValueBytes);

        return PrivateKey.fromBytes(bytes);
    }

    protected MutableList<BlockHeader> _generateBlocks(final PrivateKeyGenerator privateKeyGenerator, final Integer blockCount, final File blocksDirectory, final List<BlockHeader> initBlocks) {
        return _generateBlocks(privateKeyGenerator, blockCount, blocksDirectory, initBlocks, null);
    }

    protected BlockTemplate _getBlockTemplate(final File blocksDirectory, final Sha256Hash previousBlockHash, final Long nextBlockHeight, final TransactionGenerator transactionGenerator, final PrivateTestNetDifficultyCalculatorContext difficultyCalculatorContext, final List<BlockHeader> createdBlocks) {
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
        final BlockTemplate blockTemplate = _createBlockTemplate(previousBlockHash, nextBlockHeight, transactions, difficultyCalculatorContext);

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

    protected static class StratumServer extends BitcoinCoreStratumServer {
        public StratumServer(final BitcoinMiningRpcConnectorFactory rpcConnectionFactory, final Integer stratumPort, final ThreadPool threadPool, final MasterInflater masterInflater) {
            super(rpcConnectionFactory, stratumPort, threadPool, masterInflater);
        }

        @Override
        public void setCoinbaseAddress(final Address address) {
            super.setCoinbaseAddress(address);
        }

        public void rebuildBlockTemplate() {
            _rebuildBlockTemplate();
        }

        public void abandonMiningTasks() {
            _blockTemplate = null;
            _abandonMiningTasks();
        }
    }

    protected MutableList<BlockHeader> _generateBlocks(final PrivateKeyGenerator privateKeyGenerator, final Integer blockCount, final File blocksDirectory, final List<BlockHeader> initBlocks, final TransactionGenerator transactionGenerator) {
        final AddressInflater addressInflater = new AddressInflater();
        final BlockDeflater blockDeflater = new BlockDeflater();
        final MasterInflater masterInflater = new CoreInflater();
        final CachedThreadPool threadPool = new CachedThreadPool(32, 10000L);

        final int initBlockCount = initBlocks.getCount();
        final BlockHeader lastInitBlock = initBlocks.get(initBlockCount - 1);
        final MutableList<BlockHeader> createdBlocks = new MutableList<>(blockCount);

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

        final StratumServer stratumServer = new StratumServer(rpcConnectorFactory, 3333, threadPool, masterInflater);
        stratumServerContainer.value = stratumServer;

        {
            final int blocksCount = initBlocks.getCount();
            final BlockHeader previousBlock = initBlocks.get(blocksCount - 1);
            final Sha256Hash previousBlockHash = previousBlock.getHash();

            final Long nextBlockHeight = (long) blocksCount;
            // final List<Transaction> transactions = ((transactionGenerator != null) ? transactionGenerator.getTransactions(nextBlockHeight) : new MutableList<>(0));
            blockTemplateContainer.value = _getBlockTemplate(blocksDirectory, previousBlockHash, nextBlockHeight, transactionGenerator, difficultyCalculatorContext, createdBlocks); // _createBlockTemplate(previousBlockHash, nextBlockHeight, transactions, difficultyCalculatorContext);

            final PrivateKey privateKey = privateKeyGenerator.getCoinbasePrivateKey(nextBlockHeight);
            final Address address = addressInflater.fromPrivateKey(privateKey, true);
            stratumServer.setCoinbaseAddress(address);
            stratumServer.rebuildBlockTemplate();
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
                Logger.info("Height: " + blockHeight + " " + blockHash + " " + file.getPath());

                _sendBlock(block);

                if (newCreatedBlockCount >= blockCount) {
                    synchronized (createdBlocks) {
                        createdBlocks.notifyAll();
                        return;
                    }
                }

                final long nextBlockHeight = (blockHeight + 1L);
                // final List<Transaction> transactions = ((transactionGenerator != null) ? transactionGenerator.getTransactions(nextBlockHeight) : new MutableList<>(0));
                blockTemplateContainer.value = _getBlockTemplate(blocksDirectory, blockHash, nextBlockHeight, transactionGenerator, difficultyCalculatorContext, createdBlocks); // _createBlockTemplate(blockHash, nextBlockHeight, transactions, difficultyCalculatorContext);
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

    protected MutableList<BlockHeader> _loadInitBlocks(final File blocksBaseDirectory) {
        final MutableList<BlockHeader> initBlocks = new MutableList<>();

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
            initBlocks.add(blockHeader);

            _sendBlock(block);
        }

        Logger.debug(initBlocks.getCount() + " blocks loaded.");
        Logger.debug("HeadBlock=" + initBlocks.get(initBlocks.getCount() - 1).getHash());

        return initBlocks;
    }

    public Main() { }

    protected BlockTemplate _createBlockTemplate(final Sha256Hash previousBlockHash, final Long blockHeight, final List<Transaction> transactions, final DifficultyCalculatorContext difficultyCalculatorContext) {
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

    protected void _sendBlock(final Block block) {
        Logger.debug("Sending: " + block.getHash());
        if (SKIP_MINING) { return; }

        final boolean isBitcoinVerdeNode = false;
        if (isBitcoinVerdeNode) {
            final BitcoinNodeRpcAddress bitcoinNodeRpcAddress = new BitcoinNodeRpcAddress("localhost", 18334);
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
        Logger.debug("Sent: " + block.getHash());
    }

    protected static MutableList<Transaction> createFanOutTransactions(final Transaction rootTransactionToSpend, final PrivateKey privateKey, final Long blockHeight) throws Exception {
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

    protected static class TransactionWithBlockHeight {
        public final Transaction transaction;
        public final Long blockHeight;

        public TransactionWithBlockHeight(final Transaction transaction, final Long blockHeight) {
            this.transaction = transaction;
            this.blockHeight = blockHeight;
        }
    }

    protected MutableList<Transaction> _createFanInTransactions(final List<TransactionWithBlockHeight> transactionsToSpend, final Long blockHeight) throws Exception {
        final Long maxBlockSize = 256L * ByteUtil.Unit.Si.MEGABYTES;

        final AddressInflater addressInflater = new AddressInflater();

        final NanoTimer nanoTimer = new NanoTimer();

        nanoTimer.start();
        final ConcurrentLinkedQueue<Transaction> transactions = new ConcurrentLinkedQueue<>();

        final TransactionDeflater transactionDeflater = new TransactionDeflater();
        final AtomicLong blockSize = new AtomicLong(BlockHeaderInflater.BLOCK_HEADER_BYTE_COUNT + 8);

        final int batchSize = 32;
        final BatchRunner<TransactionWithBlockHeight> batchRunner = new BatchRunner<>(batchSize, true, 4);
        batchRunner.run(transactionsToSpend, new BatchRunner.Batch<>() {
            @Override
            public void run(final List<TransactionWithBlockHeight> batchItems) throws Exception {
                if (blockSize.get() >= maxBlockSize) { return; }

                final NanoTimer nanoTimer = new NanoTimer();
                nanoTimer.start();

                final Wallet wallet = new Wallet();

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

                final Long amount = wallet.getBalance();
                final PrivateKey privateKey = Main.derivePrivateKey(blockHeight, amount);
                final Address address = addressInflater.fromPrivateKey(privateKey, true);

                final Long fees = 0L; // wallet.calculateFees(1, outputsToSpendCount);
                wallet.setSatoshisPerByteFee(0D);

                final MutableList<PaymentAmount> paymentAmounts = new MutableList<>();
                paymentAmounts.add(new PaymentAmount(address, amount - fees));

                final Transaction transaction = wallet.createTransaction(paymentAmounts, address);
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

    protected MutableList<Transaction> _createSteadyStateTransactions(final List<TransactionWithBlockHeight> transactionsToSpend, final Long blockHeight) throws Exception {
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

                long fee = 500;
                final long maxFee = 100000L;

                int outputsToSpendCount = 0;
                for (final TransactionWithBlockHeight transactionWithBlockHeight : batchItems) {
                    final Wallet wallet = new Wallet();

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

                        if (amount1 < 1L) { break; }

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

    protected static Sha256Hash getBlockHash(final Long blockHeight, final List<BlockHeader> blockHeaders, final List<BlockHeader> newBlockHeaders) {
        if (blockHeight < blockHeaders.getCount()) {
            final int blockHeightInt = blockHeight.intValue();
            final BlockHeader blockHeader = blockHeaders.get(blockHeightInt);
            return blockHeader.getHash();
        }

        final int index = (int) (blockHeight - blockHeaders.getCount());
        final BlockHeader blockHeader = newBlockHeaders.get(index);
        return blockHeader.getHash();
    }

    public void run() {
        final Integer coinbaseMaturityBlockCount = 100;

        final MutableList<BlockHeader> blockHeaders = new MutableList<>(0);

        final File blocksBaseDirectory = new File("data/blocks");
        final List<BlockHeader> initBlocks = _loadInitBlocks(blocksBaseDirectory);
        blockHeaders.addAll(initBlocks);

        final PrivateKeyGenerator privateKeyGenerator = new PrivateKeyGenerator() {
            @Override
            public PrivateKey getCoinbasePrivateKey(final Long blockHeight) {
                return Main.derivePrivateKey(blockHeight, 50L * Transaction.SATOSHIS_PER_BITCOIN);
            }
        };

        final File defaultScenarioDirectory = new File(blocksBaseDirectory, "default");
        final File manifestFile = new File(defaultScenarioDirectory, "manifest.json");
        if (manifestFile.exists()) {
            final Json blocksManifestJson = Json.parse(StringUtil.bytesToString(IoUtil.getFileContents(manifestFile)));
            for (int i = 0; i < blocksManifestJson.length(); ++i) {
                final Sha256Hash blockHash = Sha256Hash.fromHexString(blocksManifestJson.getString(i));

                final Block block = _loadBlock(blockHash, defaultScenarioDirectory);
                _sendBlock(block);

                final BlockHeader blockHeader = new ImmutableBlockHeader(block);
                blockHeaders.add(blockHeader);
            }
        }
        else {
            final Json manifestJson = new Json(true);

            Logger.info("Generating spendable coinbase blocks.");
            final Long firstScenarioBlockHeight = (long) blockHeaders.getCount();
            final MutableList<BlockHeader> scenarioBlocks = _generateBlocks(privateKeyGenerator, coinbaseMaturityBlockCount, defaultScenarioDirectory, blockHeaders);
            blockHeaders.addAll(scenarioBlocks);
            for (final BlockHeader blockHeader : scenarioBlocks) {
                final Sha256Hash blockHash = blockHeader.getHash();
                manifestJson.add(blockHash);
            }

            Logger.info("Generating fan-out blocks.");
            final Long firstFanOutBlockHeight = (long) blockHeaders.getCount();
            final List<BlockHeader> fanOutBlocks = _generateBlocks(privateKeyGenerator, 10, defaultScenarioDirectory, blockHeaders, new TransactionGenerator() {
                @Override
                public List<Transaction> getTransactions(final Long blockHeight, final List<BlockHeader> createdBlocks) {
                    final long coinbaseToSpendBlockHeight = (blockHeight - coinbaseMaturityBlockCount); // preFanOutBlock...
                    final BlockHeader blockHeader = blockHeaders.get((int) coinbaseToSpendBlockHeight);
                    final Block block = _loadBlock(blockHeader.getHash(), defaultScenarioDirectory);

                    final Transaction transactionToSplit = block.getCoinbaseTransaction();
                    final PrivateKey coinbasePrivateKey = privateKeyGenerator.getCoinbasePrivateKey(coinbaseToSpendBlockHeight);

                    // Spend the coinbase
                    final Transaction transaction;
                    final PrivateKey transactionPrivateKey;
                    {
                        final Wallet wallet = new Wallet();
                        wallet.addPrivateKey(coinbasePrivateKey);
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

                        transaction = wallet.createTransaction(paymentAmounts, address);
                        if (transaction == null) {
                            Logger.warn("Unable to create transaction.");
                        }

                        transactionPrivateKey = coinbasePrivateKey;
                    }

                    try {
                        final MutableList<Transaction> transactions = Main.createFanOutTransactions(transaction, transactionPrivateKey, blockHeight);
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
            for (final BlockHeader blockHeader : fanOutBlocks) {
                final Sha256Hash blockHash = blockHeader.getHash();
                manifestJson.add(blockHash);
            }

            Logger.info("Generating steady-state blocks.");
            final Long firstSteadyStateBlockHeight = (long) blockHeaders.getCount();
            final List<BlockHeader> steadyStateBlocks = _generateBlocks(privateKeyGenerator, 5, defaultScenarioDirectory, blockHeaders, new TransactionGenerator() {
                @Override
                public List<Transaction> getTransactions(final Long blockHeight, final List<BlockHeader> createdBlocks) {
                    final MutableList<TransactionWithBlockHeight> transactionsToSpend = new MutableList<>();
                    {
                        final StringBuilder stringBuilder = new StringBuilder("blockHeight=" + blockHeight);
                        // Spending only first half of each 10 fan-out blocks, over 5 steady-state blocks, means each steady state block spends 1/5 of 1/2 of each fan-out block.
                        final int steadyStateBlockIndex = (int) (blockHeight - firstSteadyStateBlockHeight);
                        stringBuilder.append(" steadyStateBlockIndex=" + steadyStateBlockIndex);
                        for (int i = 0; i < 10; ++i) {
                            stringBuilder.append(" (");
                            final long blockHeightToSpend = (firstFanOutBlockHeight + i); // (firstFanOutBlockHeight + steadyStateBlockIndex + i)
                            final Sha256Hash blockHash = Main.getBlockHash(blockHeightToSpend, blockHeaders, createdBlocks);
                            final Block blockToSpend = _loadBlock(blockHash, defaultScenarioDirectory);
                            stringBuilder.append("blockHeightToSpend=" + blockHeightToSpend + " blockHash=" + blockHash);

                            final int transactionCount = blockToSpend.getTransactionCount();
                            stringBuilder.append(" transactionCount=" + transactionCount);
                            final int transactionCountToSpend = ((transactionCount / 2) / 5); // (transactionCount / 5)
                            stringBuilder.append(" transactionCountToSpend=" + transactionCountToSpend);
                            final List<Transaction> transactions = blockToSpend.getTransactions();
                            stringBuilder.append(" transactions.count=" + transactions.getCount());
                            final int startIndex = (1 + (transactionCountToSpend * steadyStateBlockIndex));
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
                        return _createSteadyStateTransactions(transactionsToSpend, blockHeight);
                    }
                    catch (final Exception exception) {
                        Logger.warn(exception);
                        return null;
                    }
                }
            });
            blockHeaders.addAll(steadyStateBlocks);
            for (final BlockHeader blockHeader : steadyStateBlocks) {
                final Sha256Hash blockHash = blockHeader.getHash();
                manifestJson.add(blockHash);
            }

            Logger.info("Generating fan-in blocks.");
            final Long firstFanInBlockHeight = (long) blockHeaders.getCount();
            final List<BlockHeader> fanInBlocks = _generateBlocks(privateKeyGenerator, 2, defaultScenarioDirectory, blockHeaders, new TransactionGenerator() {
                @Override
                public List<Transaction> getTransactions(final Long blockHeight, final List<BlockHeader> createdBlocks) {
                    final MutableList<TransactionWithBlockHeight> transactionsToSpend;
                    {
                        final long blockHeightToSpend = (firstSteadyStateBlockHeight + (blockHeight - firstFanInBlockHeight)); // spend the steady-state blocks txns in-order...
                        final Sha256Hash blockHash = blockHeaders.get((int) blockHeightToSpend).getHash();
                        final Block blockToSpend = _loadBlock(blockHash, defaultScenarioDirectory);
                        final int transactionCount = blockToSpend.getTransactionCount();
                        transactionsToSpend = new MutableList<>(transactionCount - 1);

                        final List<Transaction> transactions = blockToSpend.getTransactions();
                        for (int i = 1; i < transactionCount; ++i) {
                            final Transaction transaction = transactions.get(i);
                            transactionsToSpend.add(new TransactionWithBlockHeight(transaction, blockHeightToSpend));
                        }
                    }

                    try {
                        return _createFanInTransactions(transactionsToSpend, blockHeight);
                    }
                    catch (final Exception exception) {
                        Logger.warn(exception);
                        return null;
                    }
                }
            });
            blockHeaders.addAll(fanInBlocks);
            for (final BlockHeader blockHeader : fanInBlocks) {
                final Sha256Hash blockHash = blockHeader.getHash();
                manifestJson.add(blockHash);
            }

            Logger.info("Generating 2nd steady-state blocks.");
            final Long firstSteadyStateBlockHeightRoundTwo = (long) blockHeaders.getCount();
            final List<BlockHeader> steadyStateBlocksRoundTwo = _generateBlocks(privateKeyGenerator, 5, defaultScenarioDirectory, blockHeaders, new TransactionGenerator() {
                @Override
                public List<Transaction> getTransactions(final Long blockHeight, final List<BlockHeader> createdBlocks) {
                    final MutableList<TransactionWithBlockHeight> transactionsToSpend = new MutableList<>();
                    {
                        final int steadyStateBlockIndex = (int) (blockHeight - firstSteadyStateBlockHeightRoundTwo); // the Nth of 5 steady state blocks...

                        // Spend the respective fan-in block...
                        {
                            final long blockHeightToSpend = (firstFanInBlockHeight + steadyStateBlockIndex);
                            final Sha256Hash blockHash = Main.getBlockHash(blockHeightToSpend, blockHeaders, createdBlocks);
                            final Block blockToSpend = _loadBlock(blockHash, defaultScenarioDirectory);
                            final List<Transaction> transactions = blockToSpend.getTransactions();
                            for (int i = 1; i < transactions.getCount(); ++i) {
                                final Transaction transaction = transactions.get(i);
                                transactionsToSpend.add(new TransactionWithBlockHeight(transaction, blockHeightToSpend));
                            }
                        }

                        // And spend only second half of each 10 fan-out blocks, over 5 steady-state blocks, means each steady state block spends 1/5 of 1/2 of each fan-out block.
                        for (int i = 0; i < 10; ++i) {
                            final long blockHeightToSpend = (firstFanOutBlockHeight + steadyStateBlockIndex + i);
                            final Sha256Hash blockHash = Main.getBlockHash(blockHeightToSpend, blockHeaders, createdBlocks);
                            final Block blockToSpend = _loadBlock(blockHash, defaultScenarioDirectory);

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
                        return _createSteadyStateTransactions(transactionsToSpend, blockHeight);
                    }
                    catch (final Exception exception) {
                        Logger.warn(exception);
                        return null;
                    }
                }
            });
            blockHeaders.addAll(steadyStateBlocksRoundTwo);
            for (final BlockHeader blockHeader : steadyStateBlocksRoundTwo) {
                final Sha256Hash blockHash = blockHeader.getHash();
                manifestJson.add(blockHash);
            }

            IoUtil.putFileContents(manifestFile, StringUtil.stringToBytes(manifestJson.toString()));
        }
    }
}