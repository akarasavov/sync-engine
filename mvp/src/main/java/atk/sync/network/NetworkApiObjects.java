package atk.sync.network;

import atk.sync.model.Models;
import atk.sync.model.Operation;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static atk.sync.model.Models.*;

public class NetworkApiObjects {

    public interface NetworkRequest {
    }

    public interface NetworkResponse {
    }

    public record CheckOrderRequest(Map<SyncBucketName, List<HashCode>> operationHashCodes) implements NetworkRequest {

    }

    public record CheckOrderResponse(Map<SyncBucketName, List<HashCode>> missedOnServer,
                                     Map<SyncBucketName, List<HashCode>> missedOnClient) implements NetworkResponse {

    }

    public record PushRequest(UUID userId,
                              Map<SyncBucketName, List<Operation>> bucketOperations) implements NetworkRequest {
    }

    public record PushResponse() implements NetworkResponse {
    }

    public record PullRequest(Map<SyncBucketName, List<HashCode>> operationHashCodes) implements NetworkRequest {

    }

    public record PullResponse(List<PullResponseHelper> operations) implements NetworkResponse {

        public record PullResponseHelper(SyncBucketName syncBucket,
                                         List<Integer> hashcodeOrder,
                                         List<Operation> operations) {
        }
    }

    public record HashCode(int hash){}


    public static class ResponseWrapper implements NetworkResponse {
        private final NetworkRequest request;
        private final CompletableFuture<NetworkResponse> responseFuture;

        public ResponseWrapper(NetworkRequest request, CompletableFuture<NetworkResponse> responseFuture) {
            this.request = request;
            this.responseFuture = responseFuture;
        }

        public NetworkRequest request() {
            return request;
        }

        public void setResponseFuture(NetworkResponse response) {
            responseFuture.complete(response);
        }
    }
}
