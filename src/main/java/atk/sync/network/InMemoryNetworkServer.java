package atk.sync.network;

import java.util.concurrent.BlockingQueue;

import static atk.sync.network.NetworkApiObjects.ResponseWrapper;

public class InMemoryNetworkServer implements NetworkServer<ResponseWrapper> {

    private final BlockingQueue<ResponseWrapper> responseQueue;

    public InMemoryNetworkServer(BlockingQueue<ResponseWrapper> responseQueue) {
        this.responseQueue = responseQueue;
    }

    @Override
    public BlockingQueue<ResponseWrapper> subscribe() {
        return responseQueue;
    }
}
