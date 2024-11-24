package atk.sync.network;

import java.util.concurrent.BlockingQueue;

public interface NetworkServer<RESPONSE> {

    BlockingQueue<RESPONSE> subscribe();
}
