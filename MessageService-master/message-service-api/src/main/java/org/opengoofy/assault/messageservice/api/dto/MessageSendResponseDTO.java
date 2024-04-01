package org.opengoofy.assault.messageservice.api.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 消息发送出参实体
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MessageSendResponseDTO {
    
    /**
     * 消息 ID
     */
    private String msgId;
}
