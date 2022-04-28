package com.softwareverde.bitcoin.scaling.rpc;

import com.softwareverde.bitcoin.rpc.BitcoinNodeRpcAddress;
import com.softwareverde.bitcoin.rpc.RpcCredentials;
import com.softwareverde.bitcoin.rpc.core.BitcoinCoreRpcConnector;
import com.softwareverde.bitcoin.rpc.core.MutableRequest;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.transaction.TransactionDeflater;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.http.HttpMethod;
import com.softwareverde.http.server.servlet.response.Response;
import com.softwareverde.json.Json;
import com.softwareverde.logging.Logger;
import com.softwareverde.util.StringUtil;
import com.softwareverde.util.Util;

public class BitcoinCoreRpcConnector2 extends BitcoinCoreRpcConnector implements TransactionRpcConnector {
    public BitcoinCoreRpcConnector2(final BitcoinNodeRpcAddress bitcoinNodeRpcAddress) {
        super(bitcoinNodeRpcAddress);
    }

    public BitcoinCoreRpcConnector2(final BitcoinNodeRpcAddress bitcoinNodeRpcAddress, final RpcCredentials rpcCredentials) {
        super(bitcoinNodeRpcAddress, rpcCredentials);
    }

    @Override
    public void submitTransaction(final Transaction transaction) {
        final byte[] requestPayload;
        { // Build request payload
            final Json json = new Json(false);
            json.put("id", _nextRequestId.getAndIncrement());
            json.put("method", "sendrawtransaction");

            { // Method Parameters
                final TransactionDeflater transactionDeflater = new TransactionDeflater();
                final ByteArray transactionData = transactionDeflater.toBytes(transaction);
                final String blockDataHexString = transactionData.toString();

                final Json paramsJson = new Json(true);
                paramsJson.add(blockDataHexString.toLowerCase()); // hexdata
                // paramsJson.add(0); // dummy
                json.put("params", paramsJson);
            }

            requestPayload = StringUtil.stringToBytes(json.toString());
        }

        final MutableRequest request = new MutableRequest();
        request.setMethod(HttpMethod.POST);
        request.setRawPostData(requestPayload);

        final Response response = this.handleRequest(request, null);
        final String rawResponse = StringUtil.bytesToString(response.getContent());
        if (! Json.isJson(rawResponse)) {
            Logger.debug("Received error from " + _toString() +": " + rawResponse.replaceAll("[\\n\\r]+", "/"));
            return; // false
        }
        final Json responseJson = Json.parse(rawResponse);

        final String errorString = responseJson.getString("error");
        if (! Util.isBlank(errorString)) {
            Logger.debug("Received error from " + _toString() + ": " + errorString);
            return; // false
        }

        // Result is considered valid according to the C++ code. (string "null" or actual null are both accepted)
        // {"result": null}
        // final String resultValue = responseJson.getOrNull("result", Json.Types.STRING);
        // if (resultValue == null) { return true; }
        // return Util.areEqual("null", resultValue.toLowerCase());
    }
}
