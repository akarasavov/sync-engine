package atk.sync.agent;

import atk.sync.db.SyncBucketRepository;
import atk.sync.model.Operation;
import atk.sync.network.NetworkClient;

import java.io.Closeable;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static atk.sync.network.NetworkApiObjects.*;
import static atk.sync.network.NetworkApiObjects.CheckOrderRequest;
import static atk.sync.network.NetworkApiObjects.CheckOrderResponse;
import static atk.sync.network.NetworkApiObjects.NetworkRequest;
import static atk.sync.network.NetworkApiObjects.NetworkResponse;
import static atk.sync.network.NetworkApiObjects.PullRequest;
import static atk.sync.network.NetworkApiObjects.PullResponse;
import static atk.sync.network.NetworkApiObjects.PushRequest;
import static atk.sync.util.ExceptionUtils.wrapToRuntimeException;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SynchronizeJob implements Runnable, Closeable {
    private final NetworkClient<NetworkRequest, NetworkResponse> networkClient;
    private final SocketAddress serverAddress;
    private final List<SyncBucketRepository> syncBucketRepos;
    private final UUID myUserId;
    private final Duration RESPONSE_MAX_AWAIT = Duration.ofSeconds(10);
    private volatile boolean shouldLoop;

    public SynchronizeJob(NetworkClient<NetworkRequest, NetworkResponse> networkClient,
                          SocketAddress serverAddress,
                          UUID myUserId,
                          List<SyncBucketRepository> syncBucketRepos) {
        this.networkClient = networkClient;
        this.serverAddress = serverAddress;
        this.myUserId = myUserId;
        this.syncBucketRepos = syncBucketRepos;
    }

    @Override
    public void run() {
        while (shouldLoop) {
            synchronizeState();
        }
    }

    private void synchronizeState() {
        var bucketOperations = syncBucketRepos.stream()
                .collect(Collectors.toMap(SyncBucketRepository::tableName, SyncBucketRepository::getAllOperations));
        var operationHashCodes = bucketOperations.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> e.getValue().stream().map(Operation::hashCode).map(HashCode::new).toList()));
        //check the order of my local operations by sending list of hash(op1) .. hash(n)
        var checkOrderResponse = (CheckOrderResponse) wrapToRuntimeException(() -> networkClient.send(serverAddress, new CheckOrderRequest(operationHashCodes))
                .get(RESPONSE_MAX_AWAIT.toMillis(), MILLISECONDS));
        if (!checkOrderResponse.missedOnServer().isEmpty()) {
            var missedOnServer = checkOrderResponse.missedOnServer();
            //find representation of missed operations
            var missedOnServerOperations = missedOnServer.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> {
                var operations = bucketOperations.get(e.getKey());
                return findOperationWithTheSameHashcode(operations, e.getValue());
            }));
            //push missed operations to the server
            wrapToRuntimeException(() -> networkClient.send(serverAddress, new PushRequest(myUserId, missedOnServerOperations))
                    .get(RESPONSE_MAX_AWAIT.toMillis(), MILLISECONDS));
            return;
        }
        if (!checkOrderResponse.missedOnClient().isEmpty()) {
            var missedOnClient = checkOrderResponse.missedOnClient();
            var pullResponse = (PullResponse) wrapToRuntimeException(() ->
                    networkClient.send(serverAddress, new PullRequest(missedOnClient))
                            .get(RESPONSE_MAX_AWAIT.toMillis(), MILLISECONDS));
            //apply received operations. They should be applied based on hashorder but for LWW this can be ignored
            //TODO - take in account the hash-order
            pullResponse.operations().forEach(element -> {
            });
        }
        var response = networkClient.send(serverAddress, new PushRequest(myUserId, bucketOperations));
    }

    private List<Operation> findOperationWithTheSameHashcode(List<Operation> operations, List<HashCode> hashcodes) {
        var operationHashCodes = operations.stream().collect(Collectors.toMap(k -> new HashCode(k.hashCode()), o -> o));
        return hashcodes.stream().map(hashcode -> Objects.requireNonNull(operationHashCodes.get(hashcode))).toList();
    }

    @Override
    public void close() {
        shouldLoop = false;
    }
}
