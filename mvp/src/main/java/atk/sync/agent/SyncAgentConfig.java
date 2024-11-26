package atk.sync.agent;

import atk.sync.model.SyncRule;

import java.net.SocketAddress;
import java.util.List;
import java.util.UUID;

import static atk.sync.model.Models.JDBCPath;

public record SyncAgentConfig(List<SyncRule> syncRules,
                              SocketAddress serverAddress,
                              UUID myUserId,
                              JDBCPath jdbcPath,
                              AppVersion appVersion) {
    record AppVersion(String version) {
    }

}
