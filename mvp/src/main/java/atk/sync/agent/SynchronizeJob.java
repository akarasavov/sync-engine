package atk.sync.agent;

import atk.sync.db.SyncBucketRepository;
import atk.sync.model.Operation;
import atk.sync.model.SyncRule;
import atk.sync.network.NetworkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.SocketAddress;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static atk.sync.model.Models.*;
import static atk.sync.network.NetworkApiObjects.CheckOrderRequest;
import static atk.sync.network.NetworkApiObjects.CheckOrderResponse;
import static atk.sync.network.NetworkApiObjects.HashCode;
import static atk.sync.network.NetworkApiObjects.NetworkRequest;
import static atk.sync.network.NetworkApiObjects.NetworkResponse;
import static atk.sync.network.NetworkApiObjects.PullRequest;
import static atk.sync.network.NetworkApiObjects.PullResponse;
import static atk.sync.network.NetworkApiObjects.PushRequest;
import static atk.sync.util.ExceptionUtils.wrapToRuntimeException;
import static atk.sync.util.MetaTableReader.*;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SynchronizeJob implements Runnable, Closeable {
    private final Logger logger = LoggerFactory.getLogger(SynchronizeJob.class);
    private final NetworkClient<NetworkRequest, NetworkResponse> networkClient;
    private final List<SyncBucketRepository> syncBucketRepos;
    private final Map<SyncBucketName, Pair<TableMetaData, SyncRule>> metaDataMap;
    private final SocketAddress serverAddress;
    private final UUID myUserId;
    private final Duration RESPONSE_MAX_AWAIT = Duration.ofSeconds(10);
    private volatile boolean shouldLoop;

    public SynchronizeJob(NetworkClient<NetworkRequest, NetworkResponse> networkClient,
                          SocketAddress serverAddress,
                          UUID myUserId,
                          List<SyncBucketRepository> syncBucketRepos,
                          Map<SyncBucketName, Pair<TableMetaData, SyncRule>> metaDataMap) {
        this.networkClient = requireNonNull(networkClient);
        this.serverAddress = requireNonNull(serverAddress);
        this.myUserId = requireNonNull(myUserId);
        this.syncBucketRepos = requireNonNull(syncBucketRepos);
        this.metaDataMap = requireNonNull(metaDataMap);
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
            pullResponse.operations().forEach(response -> {
                //TODO - apply received operations. They should be applied based on hashorder but for LWW this can be ignored
//                applyReceivedOperations(response.syncBucket(), response.operations())
            });
        }
    }

    private void applyReceivedOperations(SyncBucketName syncBucketName, List<Operation> operations) {
        //1. first we need to turn off all triggers for a sync bucket
        //2. then we need to append all received operation to the sync bucket
        //3. then we need to apply all received operation to the sync future tables
        //4. then we need to turn on all triggers
        List<SqlStatement> sqlStatements = new ArrayList<>();
        var pair = metaDataMap.get(syncBucketName);
        var syncRule = pair.second();
        sqlStatements.add(syncRule.generateDeleteTriggerStatement(pair.first()));

    }

    private List<Operation> findOperationWithTheSameHashcode(List<Operation> operations, List<HashCode> hashcodes) {
        var operationHashCodes = operations.stream().collect(Collectors.toMap(k -> new HashCode(k.hashCode()), o -> o));
        return hashcodes.stream().map(hashcode -> requireNonNull(operationHashCodes.get(hashcode))).toList();
    }

    @Override
    public void close() {
        shouldLoop = false;
        logger.debug("Sync job stopped");
    }
}
