package com.softwareverde.bitcoin.scaling.rpc;

import com.softwareverde.bitcoin.rpc.BitcoinNodeRpcAddress;
import com.softwareverde.bitcoin.rpc.NodeJsonRpcConnection;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.concurrent.threadpool.CachedThreadPool;

public class VerdeTransactionRpcConnector implements TransactionRpcConnector {
    protected final BitcoinNodeRpcAddress _rpcAddress;
    protected final CachedThreadPool _threadPool;
    protected NodeJsonRpcConnection _rpcConnector;

    protected void _createRpcConnector() {
        final String host = _rpcAddress.getHost();
        final Integer port = _rpcAddress.getPort();
        _rpcConnector = new NodeJsonRpcConnection(host, port, _threadPool);
        _rpcConnector.enableKeepAlive(true);
    }

    public VerdeTransactionRpcConnector(final BitcoinNodeRpcAddress rpcAddress) {
        _rpcAddress = rpcAddress;
        _threadPool = new CachedThreadPool(4, 2000L);

        _threadPool.start();
        _createRpcConnector();
    }

    @Override
    public void submitTransaction(final Transaction transaction) {
        if (_rpcConnector == null || (! _rpcConnector.isConnected())) {
            _createRpcConnector();
        }

        _rpcConnector.submitTransaction(transaction);
    }

    @Override
    public void close() throws Exception {
        if (_rpcConnector != null) {
            _rpcConnector.close();
        }
        _threadPool.stop();
    }
}