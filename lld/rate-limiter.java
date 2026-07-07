/*
Problem Statement
Design a system that tracks and restricts the number of actions a client (user, IP, or API key) can perform within a specific timeframe, 
  rejecting excess traffic with a standard 429 Too Many Requests status code without compromising system performance.

https://www.hellointerview.com/learn/low-level-design/problem-breakdowns/rate-limiter  
*/

import lombok.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents an incoming request.
 *
 * Keeping it generic allows us to support
 * API Key, UserId, IP, etc. in future.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Request {
    private String clientId;
    private String endpoint;
}

/**
 * Immutable rule associated with an endpoint.
 *
 * Runtime state is NOT stored here.
 * Runtime state is maintained separately inside Bucket.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RateLimitRule {
    private String endpoint;
    private AlgorithmType algorithmType;
    private RateLimitConfig config;
}

/**
 * Algorithm specific configuration.
 *
 * Token Bucket:
 *  capacity
 *  refillRatePerSecond
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RateLimitConfig {
    private int capacity;
    private int refillRatePerSecond;
}

/**
 * Returned back to caller.
 *
 * Instead of returning true/false,
 * return useful metadata as well.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RateLimitResult {
    private boolean allowed;
    private int remainingTokens;
    private long retryAfterMillis;
}

public enum AlgorithmType {
    TOKEN_BUCKET,
    SLIDING_WINDOW,
    FIXED_WINDOW
}

/**
 * Strategy for identifying client.
 *
 * Different implementations:
 *
 * UserId
 * API Key
 * IP Address
 */
public interface ClientIdentifier {
    String identify(Request request);
}

public class UserIdentifier implements ClientIdentifier {
    @Override
    public String identify(Request request) {
        return request.getClientId();
    }
}

/**
 * Strategy Pattern.
 *
 * Every rate limiting algorithm
 * implements this interface.
 */
public interface RateLimitAlgorithm {
    RateLimitResult allowRequest(Request request, RateLimitRule rule);
}

/**
 * Mutable runtime state.
 *
 * Every client owns one TokenBucket.
 *
 * Runtime state is intentionally
 * separated from configuration.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TokenBucket {
    private double tokens;
    private long lastRefillTimeStamp;
}

/**
 * Token Bucket implementation.
 *
 * Design Decisions:
 *
 * 1. One bucket per client.
 *
 * 2. Bucket creation is atomic using
 *    computeIfAbsent().
 *
 * 3. Synchronize only bucket instead
 *    of whole service.
 *
 * 4. Lazy refill on every request.
 *
 * Thread Safety:
 *
 * ConcurrentHashMap
 * +
 * synchronized(bucket)
 */
public class TokenBucketRateLimiter implements RateLimitAlgorithm {
    /**
     * Runtime state.
     *
     * Key:
     *
     * client:endpoint
     */
    private final ConcurrentHashMap < String, TokenBucket > buckets = new ConcurrentHashMap < > ();

    @Override
    public RateLimitResult allowRequest(Request request, RateLimitRule rule) {
        String key = request.getClientId() + ":" + request.getEndpoint();

        /**
         * Creates bucket only once
         * atomically even under
         * concurrent requests.
         */
        TokenBucket bucket = buckets.computeIfAbsent(key, k -> new TokenBucket(rule.getConfig().getCapacity(), System.currentTimeMillis()));

        /**
         * Synchronize only this client's
         * bucket.
         *
         * Different clients continue
         * concurrently.
         */
        synchronized(bucket) {
            refill(bucket, rule);

            if (bucket.getTokens() >= 1) {
                bucket.setTokens(bucket.getTokens() - 1);

                return RateLimitResult.builder()
                    .allowed(true)
                    .remainingTokens((int) bucket.getTokens())
                    .retryAfterMillis(0);
                .build();
            }

            return RateLimitResult.builder()
                .allowed(false)
                .remainingTokens(0)
                .retryAfterMillis(retryAfter(bucket, rule));
            .build();
        }
    }

    /**
     * Refill bucket lazily.
     *
     * Instead of background scheduler,
     * refill only when request arrives.
     */
    private void refill(TokenBucket bucket, RateLimitRule rule) {
        long now = System.currentTimeMillis();
        long elapsed = now - bucket.getLastRefillTimeStamp();

        double elapsedSecond = elapsed / 1000.0;
        double refillTokens = elapsedSecond * rule.getConfig().getRefillRatePerSecond();
        double tokens = Math.min(rule.getConfig().getCapacity(), refillTokens + bucket.getTokens());

        bucket.setTokens(tokens);
        bucket.setLastRefillTimeStamp(now);
    }

    /**
     * Calculates when next token
     * becomes available.
     */
    private long retryAfter(TokenBucket bucket, RateLimitRule rule) {
        double missing = 1 - bucket.getTokens();
        double retrySeconds = missing / rule.getConfig().getRefillRatePerSecond();

        return (long)(retrySeconds * 1000);
    }
}

/**
 * Provides rate limiting configuration.
 *
 * Current Implementation:
 *      In-memory configuration.
 *
 * Production:
 *      DB
 *      Config Server
 *      Dynamic Reload
 */
public class ConfigService {
    private final Map < String, RateLimitRule > rules = new HashMap < > ();

    public ConfigService() {
        rules.put("/search", new RateLimitRule("/search", AlgorithmType.TOKEN_BUCKET, new RateLimitConfig(5, 1)));
        rules.put("DEFAULT", new RateLimitRule("DEFAULT", AlgorithmType.TOKEN_BUCKET, new RateLimitConfig(10, 2)));
    }

    public RateLimitRule getRule(String endpoint) {
        return rules.getOrDefault(endpoint, rules.get("DEFAULT"));
    }
}

/**
 * Creates/returns appropriate rate limiting strategy.
 *
 * Keeps algorithm selection outside business logic.
 * Makes adding new algorithms straightforward.
 */
public class RateLimitFactory {
    private final ConcurrentHashMap < AlgorithmType, RateLimitAlgorithm > algorithms = new ConcurrentHashMap < > ();

    public RateLimitFactory() {
      /**
       * One singleton instance
       * of every algorithm.
       *
       * No object creation
       * per request.
       */
        algorithms.put(AlgorithmType.TOKEN_BUCKET, new TokenBucketRateLimiter());
        // algorithms.put(...)
    }

    public RateLimitAlgorithm get(AlgorithmType type) {
        RateLimiterAlgorithm algorithm = algorithms.get(type);

        if (algorithm == null) {
            throw new IllegalArgumentException("Unsupported algorithm");
        }

        return algorithm;
    }
}

/**
 * Entry point of Rate Limiter.
 *
 * Responsibilities:
 *
 * 1. Identify client
 * 2. Fetch configuration
 * 3. Pick algorithm
 * 4. Delegate request
 *
 * Business logic is intentionally
 * delegated to Strategy classes.
 */
@RequiredArgsConstructor
public class RateLimitService {
    private final ClientIdentifier clientIdentifier;
    private final ConfigService configService;
    private final RateLimitFactory factory;

    public RateLimitResult allowRequest(Request request) {
        String clientId = clientIdentifier.identify(request);
        request.setClientId(clientId);

        RateLimitRule rule = configService.getRule(request.getEndpoint());
        RateLimitAlgorithm algorithm = factory.get(rule.getAlgorithmType());

        return algorithm.allowRequest(request, rule);
    }
}

/**
 * Driver program.
 *
 * Simulates multiple requests
 * from same client.
 */
public class Main throws InterruptedException {
    public static void main(String[] args) {
        RateLimitService service = new RateLimitService(new UserIdentifier(), new ConfigService(), new RateLimitFactory());
        Request request = new Request("user", "/search");

        for (int i = 1; i <= 10; ++i) {
            System.out.println(service.allowRequest(request));

            Thread.sleep(200);
        }
    }
}

/*
Design Decisions (Mention these verbally)
1. Why Strategy Pattern?
"Different rate-limiting algorithms have different implementations. Strategy lets me add new algorithms without modifying the service."

2. Why ConcurrentHashMap?
"It provides thread-safe storage of buckets. computeIfAbsent() atomically creates a bucket only once per client."

3. Why synchronized(bucket)?
"I only synchronize the mutable state of a single client. Different clients don't block each other."

4. Why separate RateLimitRule and TokenBucket?
"RateLimitRule is immutable configuration. TokenBucket is mutable runtime state. Keeping them separate makes the design cleaner and allows configuration to be managed independently."

5. Why lazy refill?
"Instead of running a background scheduler to refill every bucket, I calculate token replenishment only when a request arrives. This is simpler and more efficient because inactive clients don't consume CPU."
*/  
