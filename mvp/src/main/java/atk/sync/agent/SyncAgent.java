package atk.sync.agent;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Objects.requireNonNull;

public class SyncAgent implements Closeable {

    private final SyncAgentConfig config;
    private final ExecutorService syncAgentExecutor;

    public SyncAgent(SyncAgentConfig config) {
        this.config = requireNonNull(config);
        this.syncAgentExecutor = Executors.newCachedThreadPool();
    }

    @Override
    public void close() throws IOException {

    }
}
