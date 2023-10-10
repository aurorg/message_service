package org.opengoofy.assault.messageservice.biz.handler.send.base;

import org.opengoofy.assault.messageservice.biz.dto.MessagePlatformSendResponseDTO;
import org.opengoofy.assault.messageservice.biz.mq.event.MessageSendEvent;

/**
 * 消息发送接口
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
public interface MessageSendService {
    
    /**
     * 消息发送
     */
    MessagePlatformSendResponseDTO send(MessageSendEvent messageSendEvent) throws Exception;
}
