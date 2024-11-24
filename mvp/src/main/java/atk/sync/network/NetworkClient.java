package atk.sync.network;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;

public interface NetworkClient<REQUEST, RESPONSE> {

    CompletableFuture<RESPONSE> send(SocketAddress target, REQUEST request);

}
