package com.softwareverde.bitcoin.scaling.generate;

import com.softwareverde.bitcoin.address.Address;
import com.softwareverde.bitcoin.inflater.MasterInflater;
import com.softwareverde.bitcoin.rpc.BitcoinMiningRpcConnectorFactory;
import com.softwareverde.bitcoin.stratum.BitcoinCoreStratumServer;
import com.softwareverde.concurrent.threadpool.ThreadPool;

public class StratumServer extends BitcoinCoreStratumServer {
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