package org.opengoofy.assault.messageservice.biz.common;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.Objects;

import static org.opengoofy.assault.messageservice.biz.common.MessageConstants.SMS_MESSAGE_KEY;

/**
 * 消息类型枚举
 */
@RequiredArgsConstructor
public enum MessageTypeEnum {
    
    /**
     * 短信验证码消息
     */
    SMS_VERIFICATION_MESSAGE(0, SMS_MESSAGE_KEY),
    
    /**
     * 短信通知消息
     */
    SMS_INFORM_MESSAGE(1, SMS_MESSAGE_KEY),
    
    /**
     * 短信营销消息
     */
    SMS_MARKETING_MESSAGE(2, SMS_MESSAGE_KEY),
    
    /**
     * 微信模板消息
     */
    WE_CHART_MESSAGE(3, "WE_CHART_TEMPLATE_MESSAGE"),
    
    /**
     * 邮件消息
     */
    MAIL_MESSAGE(4, "MAIL_MESSAGE");
    
    @Getter
    private final Integer type;
    
    @Getter
    private final String platform;
    
    /**
     * 根据 type 获取 platform
     */
    public static String getPlatformByType(Integer type) {
        return Arrays.stream(MessageTypeEnum.values())
                .filter(each -> Objects.equals(type, each.getType())).findFirst()
                .map(MessageTypeEnum::getPlatform)
                .orElseThrow(() -> new RuntimeException());
    }
}
