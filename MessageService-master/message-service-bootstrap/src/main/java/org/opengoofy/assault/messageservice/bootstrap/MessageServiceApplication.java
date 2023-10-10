package org.opengoofy.assault.messageservice.bootstrap;

import cn.hippo4j.core.enable.EnableDynamicThreadPool;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;

@EnableDynamicThreadPool
@SpringBootApplication(scanBasePackages = "org.opengoofy.assault.messageservice")
@MapperScan("org.opengoofy.assault.messageservice.biz.dao.mapper")
@EnableFeignClients("org.opengoofy.assault.messageservice.biz.remote")
public class MessageServiceApplication {
    
    public static void main(String[] args) {
        SpringApplication.run(MessageServiceApplication.class, args);
    }
}
