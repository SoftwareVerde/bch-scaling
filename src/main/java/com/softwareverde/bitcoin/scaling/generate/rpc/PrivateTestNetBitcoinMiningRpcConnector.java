package com.softwareverde.bitcoin.scaling.generate.rpc;

import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.rpc.BitcoinMiningRpcConnector;
import com.softwareverde.bitcoin.rpc.BlockTemplate;
import com.softwareverde.bitcoin.rpc.RpcNotificationCallback;
import com.softwareverde.bitcoin.rpc.RpcNotificationType;
import com.softwareverde.bitcoin.rpc.monitor.Monitor;
import com.softwareverde.http.server.servlet.request.Request;
import com.softwareverde.http.server.servlet.response.Response;
import com.softwareverde.util.Container;

public class PrivateTestNetBitcoinMiningRpcConnector implements BitcoinMiningRpcConnector {
    protected static class FakeMonitor implements Monitor {
        @Override
        public Boolean isComplete() { return true; }

        @Override
        public Long getDurationMs() { return 0L; }

        @Override
        public void setMaxDurationMs(final Long maxDurationMs) { }

        @Override
        public void cancel() { }
    }

    protected final Container<BlockTemplate> _blockTemplate;

    public PrivateTestNetBitcoinMiningRpcConnector(final Container<BlockTemplate> blockTemplateContainer) {
        _blockTemplate = blockTemplateContainer;
    }

    @Override
    public BlockTemplate getBlockTemplate(final Monitor monitor) {
        return _blockTemplate.value;
    }

    @Override
    public Boolean validateBlockTemplate(final Block block, final Monitor monitor) {
        return true;
    }

    @Override
    public Boolean submitBlock(final Block block, final Monitor monitor) {
        return true;
    }

    @Override
    public Boolean supportsNotifications() {
        return false;
    }

    @Override
    public Boolean supportsNotification(final RpcNotificationType rpcNotificationType) {
        return false;
    }

    @Override
    public void subscribeToNotifications(final RpcNotificationCallback rpcNotificationCallback) { }

    @Override
    public void unsubscribeToNotifications() { }

    @Override
    public String getHost() {
        return "localhost";
    }

    @Override
    public Integer getPort() {
        return 8334;
    }

    @Override
    public Monitor getMonitor() {
        return new FakeMonitor();
    }

    @Override
    public Response handleRequest(final Request request, final Monitor monitor) {
        return new Response();
    }

    @Override
    public void close() { }
}
