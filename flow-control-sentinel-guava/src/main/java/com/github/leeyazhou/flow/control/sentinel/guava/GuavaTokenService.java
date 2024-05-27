/**
 * 
 */
package com.github.leeyazhou.flow.control.sentinel.guava;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import com.alibaba.csp.sentinel.cluster.TokenResult;
import com.alibaba.csp.sentinel.cluster.TokenResultStatus;
import com.alibaba.csp.sentinel.cluster.TokenService;
import com.alibaba.csp.sentinel.cluster.flow.ConcurrentClusterFlowChecker;
import com.alibaba.csp.sentinel.cluster.flow.rule.ClusterFlowRuleManager;
import com.alibaba.csp.sentinel.cluster.flow.rule.ClusterParamFlowRuleManager;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.param.ParamFlowRule;
import com.alibaba.csp.sentinel.spi.Spi;
import com.google.common.util.concurrent.RateLimiter;

/**
 * @author leeyazhou
 */
@Spi(order = -1)
public class GuavaTokenService implements TokenService {
    private static final ConcurrentMap<Long, RateLimiter> rateLimiterMap = new ConcurrentHashMap<>();

    @Override
    public TokenResult requestToken(Long ruleId, int acquireCount, boolean prioritized) {
        if (notValidRequest(ruleId, acquireCount)) {
            return badRequest();
        }
        // The rule should be valid.
        FlowRule rule = ClusterFlowRuleManager.getFlowRuleById(ruleId);
        if (rule == null) {
            return new TokenResult(TokenResultStatus.NO_RULE_EXISTS);
        }
        RateLimiter rateLimiter = getRateLimiter(rule.getId(), rule.getWarmUpPeriodSec(), rule.getCount());

        boolean acquired = rateLimiter.tryAcquire();
        if (acquired == false) {
            return new TokenResult(TokenResultStatus.TOO_MANY_REQUEST);
        }
        return new TokenResult(TokenResultStatus.OK);
    }

    private RateLimiter getRateLimiter(Long ruleId, int warmUpInSecond, double count) {
        return rateLimiterMap.compute(ruleId, (k, v) -> {
            if (v == null) {
                v = RateLimiter.create(count, Duration.ofSeconds(warmUpInSecond));
            }
            return v;
        });
    }

    @Override
    public TokenResult requestParamToken(Long ruleId, int acquireCount, Collection<Object> params) {
        if (notValidRequest(ruleId, acquireCount) || params == null || params.isEmpty()) {
            return badRequest();
        }
        // The rule should be valid.
        ParamFlowRule rule = ClusterParamFlowRuleManager.getParamRuleById(ruleId);
        if (rule == null) {
            return new TokenResult(TokenResultStatus.NO_RULE_EXISTS);
        }
        RateLimiter rateLimiter = getRateLimiter(rule.getId(), 10, rule.getCount());

        boolean acquired = rateLimiter.tryAcquire();
        if (acquired == false) {
            return new TokenResult(TokenResultStatus.TOO_MANY_REQUEST);
        }
        return new TokenResult(TokenResultStatus.OK);
    }

    @Override
    public TokenResult requestConcurrentToken(String clientAddress, Long ruleId, int acquireCount) {
        if (notValidRequest(clientAddress, ruleId, acquireCount)) {
            return badRequest();
        }
        // The rule should be valid.
        FlowRule rule = ClusterFlowRuleManager.getFlowRuleById(ruleId);
        if (rule == null) {
            return new TokenResult(TokenResultStatus.NO_RULE_EXISTS);
        }
        return ConcurrentClusterFlowChecker.acquireConcurrentToken(clientAddress, rule, acquireCount);
    }

    @Override
    public void releaseConcurrentToken(Long tokenId) {
        if (tokenId == null) {
            return;
        }
        ConcurrentClusterFlowChecker.releaseConcurrentToken(tokenId);
    }

    private boolean notValidRequest(Long id, int count) {
        return id == null || id <= 0 || count <= 0;
    }

    private boolean notValidRequest(String address, Long id, int count) {
        return address == null || "".equals(address) || id == null || id <= 0 || count <= 0;
    }

    private TokenResult badRequest() {
        return new TokenResult(TokenResultStatus.BAD_REQUEST);
    }

}
