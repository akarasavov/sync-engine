package atk.sync.network;

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

import static atk.sync.network.NetworkApiObjects.NetworkRequest;
import static atk.sync.network.NetworkApiObjects.NetworkResponse;
import static atk.sync.network.NetworkApiObjects.ResponseWrapper;
import static atk.sync.util.ExceptionUtils.wrapToRuntimeException;

public class InMemoryNetworkClient implements NetworkClient<NetworkRequest, NetworkResponse> {

    private final Map<SocketAddress, BlockingQueue<ResponseWrapper>> incomingQueue;

    public InMemoryNetworkClient(Map<SocketAddress, BlockingQueue<ResponseWrapper>> incomingQueue) {
        this.incomingQueue = incomingQueue;
    }

    @Override
    public CompletableFuture<NetworkResponse> send(SocketAddress target, NetworkRequest request) {
        var incomingQueue = this.incomingQueue.get(target);
        if (incomingQueue != null) {
            var responseFuture = new CompletableFuture<NetworkResponse>();
            wrapToRuntimeException(() -> incomingQueue.put(new ResponseWrapper(request, responseFuture)));
            return responseFuture;
        }
        throw new IllegalArgumentException("Can't find incoming channel for " + target);
    }
}
