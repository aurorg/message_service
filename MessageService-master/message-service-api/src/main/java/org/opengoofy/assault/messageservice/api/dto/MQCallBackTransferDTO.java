package org.opengoofy.assault.messageservice.api.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * MQ 回调传递实体
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MQCallBackTransferDTO {
    
    // -------------------------------------------------------------------------
    // 消息发送结果，扩展参数
    // -------------------------------------------------------------------------
    
    /**
     * 发送状态
     */
    private Boolean success;
    
    /**
     * 错误信息
     */
    private String errorMsg;
    
    /**
     * 消息发送 ID
     */
    private String msgId;
    
    // -------------------------------------------------------------------------
    // 调用方传递参数，固定返回
    // -------------------------------------------------------------------------
    
    /**
     * 客户端传递入参
     */
    private MessageSendRequestDTO messageSendRequest;
}
