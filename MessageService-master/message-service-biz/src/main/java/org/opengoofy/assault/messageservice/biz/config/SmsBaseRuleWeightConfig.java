package org.opengoofy.assault.messageservice.biz.config;

import lombok.Data;

import java.util.List;

/**
 * 基础短信权重配置接口
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
public interface SmsBaseRuleWeightConfig {
    
    /**
     * 获取权重配置集合
     */
    List<Weight> getWeightList();
    
    @Data
    class Weight {
        
        /**
         * 渠道商 ID
         */
        private String channelId;
        
        /**
         * 配置权重占比，0 - 10
         */
        private String weight;
    }
}
