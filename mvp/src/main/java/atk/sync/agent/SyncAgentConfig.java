package atk.sync.agent;

import atk.sync.model.SyncRule;

import java.net.SocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

public record SyncAgentConfig(List<SyncRule> syncRules,
                              SocketAddress serverAddress,
                              UUID myUserId,
                              Path jdbcPathToDb) {

}
