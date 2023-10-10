package org.opengoofy.assault.messageservice.biz.service;

import org.opengoofy.assault.messageservice.api.dto.MessageSendRequestDTO;
import org.opengoofy.assault.messageservice.api.dto.MessageSendResponseDTO;

/**
 * 消息发送接口
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
public interface MessageSendService {
    
    /**
     * 消息发送
     *
     * @param requestParam 消息发送入参实体
     * @return 消息发送返回结果
     */
    MessageSendResponseDTO messageSend(MessageSendRequestDTO requestParam);

    /**
     * 同步消息发送
     *
     * @param requestParam 消息发送入参实体
     * @return 消息发送返回结果
     */
    MessageSendResponseDTO syncMessageSend(MessageSendRequestDTO requestParam);
}
