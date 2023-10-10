package org.opengoofy.assault.messageservice.biz.common;

/**
 * 消息缓存常量类
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
public class MessageCacheConstants {
    
    /**
     * 消息模板缓存前缀 Key
     */
    public static final String MESSAGE_TEMPLATE_CACHE_PREFIX_KEY = "template:message_template_";
    
    /**
     * 消息模板参数缓存前缀 Key
     */
    public static final String MESSAGE_TEMPLATE_PARAM_CACHE_PREFIX_KEY = "template:message_template_param_";
    
    /**
     * 默认过期时间（一月），单位毫秒
     */
    public static final long DEFAULT_CACHE_TIMOUT = 2626560000L;
}
