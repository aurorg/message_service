package org.opengoofy.assault.messageservice.biz.mq.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.opengoofy.assault.messageservice.api.dto.MessageSendRequestDTO;

import java.util.List;

/**
 * 消息发送事件
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageSendEvent {
    
    /**
     * 消息ID
     */
    private String msgId;
    
    /**
     * 消息发送请求入参
     */
    private MessageSendRequestDTO messageSendRequest;
    
    /**
     * 短信发送可选择的渠道，短信消息专属
     */
    private transient List<String> smsOptionalChannels;
    
    /**
     * 当前发送渠道，短信消息专属
     */
    private transient String currentSendChannel;
    
    /**
     * 删除短信可选择的渠道并设置为当前发送渠道，短信消息专属
     */
    public void removeAndSetSmsChannel(String channel) {
        smsOptionalChannels.remove(channel);
        currentSendChannel = channel;
    }
}
