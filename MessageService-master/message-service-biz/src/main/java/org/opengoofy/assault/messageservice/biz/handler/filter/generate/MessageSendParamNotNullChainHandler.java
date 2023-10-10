package org.opengoofy.assault.messageservice.biz.handler.filter.generate;

import com.google.common.base.Strings;
import org.opengoofy.assault.framework.starter.convention.exception.ClientException;
import org.opengoofy.assault.messageservice.api.dto.MessageSendRequestDTO;
import org.springframework.stereotype.Component;

import java.util.Objects;

/**
 * 消息发送生成判断参数不为空
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Component
public class MessageSendParamNotNullChainHandler implements MessageSendChainFilter<MessageSendRequestDTO> {
    
    @Override
    public void handler(MessageSendRequestDTO requestParam) {
        if (Strings.isNullOrEmpty(requestParam.getTemplateId())) {
            throw new ClientException("模板ID不能为空或空的字符串");
        }
        if (Strings.isNullOrEmpty(requestParam.getSourceId())) {
            throw new ClientException("来源ID不能为空或空的字符串");
        }
        if (Strings.isNullOrEmpty(requestParam.getReceiver())) {
            throw new ClientException("接收者不能为空或空的字符串");
        }
        if (Objects.isNull(requestParam.getMsgType())) {
            throw new ClientException("消息类型不能为空或空的字符串");
        }
    }
    
    @Override
    public int getOrder() {
        return 0;
    }
}
