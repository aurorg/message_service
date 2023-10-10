package org.opengoofy.assault.messageservice.config;

import cn.hippo4j.core.executor.DynamicThreadPool;
import cn.hippo4j.core.executor.support.ThreadPoolBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ThreadPoolExecutor;

@Configuration
public class ThreadPoolConfiguration {
    
    @Bean
    @DynamicThreadPool
    public ThreadPoolExecutor otherMessageConsumeDynamicExecutor() {
        String threadPoolId = "other-message-consume";
        return ThreadPoolBuilder.builder()
                .threadFactory(threadPoolId)
                .threadPoolId(threadPoolId)
                .dynamicPool()
                .build();
    }
    
    @Bean
    @DynamicThreadPool
    public ThreadPoolExecutor smsVerificationMessageConsumeDynamicExecutor() {
        String threadPoolId = "sms-verification-message-consume";
        return ThreadPoolBuilder.builder()
                .threadFactory(threadPoolId)
                .threadPoolId(threadPoolId)
                .dynamicPool()
                .build();
    }
    
    /**
     * 非动态线程池，留下一道思考题，自己改造为 Hippo4j 动态线程池
     */
    @Bean
    @DynamicThreadPool
    public ThreadPoolExecutor messageReceiptExecutor() {
        return ThreadPoolBuilder.builder().threadFactory("message-receipt").build();
    }
}
