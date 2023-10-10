package org.opengoofy.assault.messageservice.biz.service;

import org.opengoofy.assault.messageservice.api.dto.MessageQueryRequestDTO;
import org.opengoofy.assault.messageservice.api.dto.MessageQueryResponseDTO;

import java.util.List;

/**
 * 消息查询接口
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
public interface MessageQueryService {
    
    /**
     * 消息结果查询
     *
     * @param requestParam 消息查询入参实体
     * @return 消息查询返回结果
     */
    List<MessageQueryResponseDTO> messageQuery(MessageQueryRequestDTO requestParam);
}
