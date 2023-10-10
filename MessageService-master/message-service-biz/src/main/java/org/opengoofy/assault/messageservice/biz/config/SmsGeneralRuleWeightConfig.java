package org.opengoofy.assault.messageservice.biz.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 通知短信发送渠道商权重配置
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Data
@Component
@ConfigurationProperties(prefix = "message.sms.weight.general")
public class SmsGeneralRuleWeightConfig implements SmsBaseRuleWeightConfig {
    
    /**
     * 渠道权重集合
     */
    private List<SmsBaseRuleWeightConfig.Weight> weightList;
}
