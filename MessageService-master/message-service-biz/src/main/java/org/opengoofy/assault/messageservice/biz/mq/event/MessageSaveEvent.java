package org.opengoofy.assault.messageservice.biz.mq.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.opengoofy.assault.messageservice.api.dto.MessageSendRequestDTO;
import org.opengoofy.assault.messageservice.biz.dto.MessagePlatformSendResponseDTO;

/**
 * 消息保存事件
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageSaveEvent {
    
    /**
     * 消息ID
     */
    private String msgId;
    
    /**
     * 当前发送渠道，短信消息专属
     */
    private String currentSendChannel;
    
    /**
     * 消息发送请求入参
     */
    private MessageSendRequestDTO messageSendRequest;
    
    /**
     * 消息实际发送三方平台响应实体
     */
    private MessagePlatformSendResponseDTO messagePlatformSendResponse;
}
