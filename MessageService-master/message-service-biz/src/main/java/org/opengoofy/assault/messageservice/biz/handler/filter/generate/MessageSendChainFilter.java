package org.opengoofy.assault.messageservice.biz.handler.filter.generate;

import org.opengoofy.assault.messageservice.api.dto.MessageSendRequestDTO;
import org.opengoofy.assault.messageservice.biz.common.MessageChainMarkEnum;
import org.opengoofy.assault.messageservice.biz.handler.filter.base.AbstractChainHandler;

/**
 * 消息发送责任链过滤器
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
public interface MessageSendChainFilter<T extends MessageSendRequestDTO> extends AbstractChainHandler<MessageSendRequestDTO> {
    
    @Override
    default String mark() {
        return MessageChainMarkEnum.MESSAGE_SEND_FILTER.name();
    }
}
