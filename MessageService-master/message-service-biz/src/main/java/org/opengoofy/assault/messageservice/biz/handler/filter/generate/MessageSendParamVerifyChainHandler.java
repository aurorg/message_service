package org.opengoofy.assault.messageservice.biz.handler.filter.generate;

import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.lang.Validator;
import lombok.RequiredArgsConstructor;
import org.opengoofy.assault.framework.starter.convention.exception.ClientException;
import org.opengoofy.assault.messageservice.api.dto.MessageSendRequestDTO;
import org.opengoofy.assault.messageservice.biz.common.MessageTypeEnum;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 消息发送判断参数是否正确
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Component
@RequiredArgsConstructor
public class MessageSendParamVerifyChainHandler implements MessageSendChainFilter<MessageSendRequestDTO> {

    // 定义合法的短信类型列表
    private final List<Integer> smsTypes = ListUtil.of(MessageTypeEnum.SMS_VERIFICATION_MESSAGE.getType(),
            MessageTypeEnum.SMS_INFORM_MESSAGE.getType(),
            MessageTypeEnum.SMS_MARKETING_MESSAGE.getType());


    /**
     * 处理器的具体处理逻辑
     *
     * @param requestParam 消息发送请求参数
     */
    @Override
    public void handler(MessageSendRequestDTO requestParam) {
        // 检查短信类型是否合法
        if (smsTypes.contains(requestParam.getMsgType())) {
            // 检查手机号是否合法
            if (!Validator.isMobile(requestParam.getReceiver())) {
                throw new ClientException("手机号不合法");
            }
        }
    }

    /**
     * 获取该处理器在责任链中的执行顺序
     *
     * @return 处理器的执行顺序
     */
    @Override
    public int getOrder() {
        return 1;
    }
}
