package org.opengoofy.assault.messageservice.biz.common;

/**
 * 消息发送常量
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
public class MessageRocketMQConstants {
    
    /**
     * 消息公共 Topic
     */
    public static final String MESSAGE_COMMON_TOPIC = "common_message-service_topic";
    
    /**
     * 消息回调 Topic
     */
    public static final String MESSAGE_CALLBACK_TOPIC = "common_message-service_callback_topic";
    
    /**
     * 短信验证码消息发送 TAG
     */
    public static final String SMS_MESSAGE_VERIFICATION_SEND_TAG = "common_message-service_sms-verification-message-send_tag";
    
    /**
     * 短信验证码消息发送 CG
     */
    public static final String SMS_MESSAGE_VERIFICATION_SEND_CG = "common_message-service_sms-verification-message-send_cg";
    
    /**
     * 其它消息发送 TAG
     */
    public static final String OTHER_MESSAGE_SEND_TAG = "common_message-service_other-message-send_tag";
    
    /**
     * 其它消息发送 CG
     */
    public static final String OTHER_MESSAGE_SEND_CG = "common_message-service_other-message-send_cg";
    
    /**
     * 通用消息保存 TAG
     */
    public static final String COMMON_MESSAGE_SAVE_TAG = "common_message-service_common-message-save_tag";
    
    /**
     * 通用消息保存 CG
     */
    public static final String COMMON_MESSAGE_SAVE_CG = "common_message-service_common-message-save_cg";
    
    /**
     * 异步回调消息发送结果 TAG 模版
     */
    public static final String CALLBACK_MESSAGE_SEND_TAG_TEMPLATE = "common_%s_%s_tag";
}
