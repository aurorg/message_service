package org.opengoofy.assault.messageservice.biz.dao.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import org.opengoofy.assault.framework.starter.database.BaseDO;

/**
 * 渠道和 Source 对应关系实体
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Data
@TableName("channel_relation")
public class ChannelRelationDO extends BaseDO {
    
    /**
     * id
     */
    private Long id;
    
    /**
     * sid
     */
    private String sourceId;
    
    /**
     * 签名
     */
    private String sign;
    
    /**
     * 自定义三方平台渠道id
     */
    private String channelId;
}
