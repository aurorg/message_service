package org.opengoofy.assault.messageservice.biz.dao.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import org.opengoofy.assault.framework.starter.database.BaseDO;

/**
 * 业务方配置实体
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Data
@TableName("source_config")
public class SourceConfigDO extends BaseDO {
    
    /**
     * id
     */
    private Long id;
    
    /**
     * sid
     */
    private String sourceId;
    
    /**
     * 渠道名
     */
    private String name;
}
