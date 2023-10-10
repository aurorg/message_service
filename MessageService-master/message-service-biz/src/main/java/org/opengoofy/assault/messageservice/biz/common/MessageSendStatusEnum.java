package org.opengoofy.assault.messageservice.biz.common;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * 消息发送状态枚举
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@RequiredArgsConstructor
public enum MessageSendStatusEnum {
    
    /**
     * 发送成功
     */
    SEND_SUCCESS(0),
    
    /**
     * 发送失败
     */
    SEND_FAIL(1),
    
    /**
     * 发送中，短信独有状态
     */
    SEND_PROGRESS(2),
    
    /**
     * 提交失败，短信独有状态
     */
    SUBMIT_FAIL(3);
    
    @Getter
    private final int code;
}
