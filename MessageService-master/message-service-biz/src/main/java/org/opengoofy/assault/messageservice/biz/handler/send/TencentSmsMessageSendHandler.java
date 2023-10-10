package org.opengoofy.assault.messageservice.biz.handler.send;

import com.alibaba.fastjson.JSON;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.opengoofy.assault.messageservice.biz.dto.MessagePlatformSendResponseDTO;
import org.opengoofy.assault.messageservice.biz.handler.send.base.AbstractMessageSendService;
import org.opengoofy.assault.messageservice.biz.handler.send.base.MessageSendService;
import org.opengoofy.assault.messageservice.biz.mq.event.MessageSendEvent;
import org.springframework.stereotype.Component;

import static org.opengoofy.assault.messageservice.biz.common.MessageConstants.SMS_MESSAGE_KEY;

/**
 * 腾讯云短信发送组件
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Slf4j
@Component
public class TencentSmsMessageSendHandler extends AbstractMessageSendService implements MessageSendService {
    
    @Override
    public String mark() {
        return SMS_MESSAGE_KEY + "TENCENT";
    }
    
    @Override
    @SneakyThrows
    public MessagePlatformSendResponseDTO executeResp(MessageSendEvent messageSendEvent) {
        // 这里直接模拟调用失败
        log.error("腾讯云短信调用失败，入参：{}", JSON.toJSONString(messageSendEvent));
        return MessagePlatformSendResponseDTO.builder().success(false).build();
    }
}
