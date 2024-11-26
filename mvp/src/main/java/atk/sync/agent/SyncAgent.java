package atk.sync.agent;

import atk.sync.db.SyncBucketRepository;
import atk.sync.model.Models;
import atk.sync.model.SyncRule;
import atk.sync.network.NetworkClient;
import atk.sync.util.ExceptionUtils;
import atk.sync.util.MetaTableReader;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static atk.sync.model.Models.JDBCPath;
import static atk.sync.network.NetworkApiObjects.NetworkRequest;
import static atk.sync.network.NetworkApiObjects.NetworkResponse;
import static java.util.Objects.requireNonNull;

public class SyncAgent implements Closeable {

    private final SyncAgentConfig config;
    private final ExecutorService syncAgentExecutor;
    private boolean stopped;
    private CompletableFuture<?> syncJobFuture;

    public SyncAgent(SyncAgentConfig config, NetworkClient<NetworkRequest, NetworkResponse> networkClient, List<SyncRule> syncRules) {
        this.config = requireNonNull(config);
        this.syncAgentExecutor = Executors.newCachedThreadPool();
        var syncJob = createSyncJob(networkClient, config.serverAddress(), config.myUserId(), config.jdbcPath(), syncRules);

        start(syncJob);
    }

    private SynchronizeJob createSyncJob(NetworkClient<NetworkRequest, NetworkResponse> networkClient,
                                         SocketAddress serverAddress,
                                         UUID myUserId,
                                         JDBCPath jdbcPath,
                                         List<SyncRule> syncRules) {
        var metaDataMap = ExceptionUtils
                .wrapToRuntimeException(() -> MetaTableReader.getTableMetadata(jdbcPath, syncRules));
        var syncBuckets = syncRules.stream().map(s -> new SyncBucketRepository(s.bucketName, jdbcPath)).toList();
        return new SynchronizeJob(networkClient, serverAddress, myUserId, syncBuckets, metaDataMap);
    }

    private synchronized void start(SynchronizeJob syncJob) {
        this.syncJobFuture = CompletableFuture.runAsync(syncJob, syncAgentExecutor);
        syncJobFuture.whenComplete((_, _) -> syncJob.close());
    }

    @Override
    public void close() throws IOException {
        if (!stopped) {
            syncJobFuture.cancel(true);
            var awaitDuration = Duration.ofSeconds(5);
            var deadline = Instant.now().plus(awaitDuration.toMillis(), ChronoUnit.MILLIS);
            while (syncAgentExecutor.shutdownNow().isEmpty() || Instant.now().isAfter(deadline)) {
                ExceptionUtils.wrapToRuntimeException(() -> Thread.sleep(100));
            }
            if (syncAgentExecutor.shutdownNow().isEmpty()) {
                throw new IllegalStateException("Wasn't able to shutdown all jobs for " + awaitDuration);
            }
        }
        stopped = true;
    }
}
