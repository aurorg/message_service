package org.opengoofy.assault.messageservice.biz.service;

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opengoofy.assault.framework.starter.common.toolkit.BeanUtil;
import org.opengoofy.assault.messageservice.api.dto.MessageQueryRequestDTO;
import org.opengoofy.assault.messageservice.api.dto.MessageQueryResponseDTO;
import org.opengoofy.assault.messageservice.biz.dao.entity.SendRecordDO;
import org.opengoofy.assault.messageservice.biz.dao.mapper.SendRecordMapper;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 消息查询接口
 *该类负责根据查询请求参数进行消息查询，并返回查询结果。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MessageQueryServiceImpl implements MessageQueryService {
    
    private final SendRecordMapper sendRecordMapper;


    /**
     * 根据查询请求参数进行消息查询
     *
     * @param requestParam 查询请求参数对象，包含了接收者、消息ID、发送时间范围等条件
     * @return 包含查询结果的消息响应DTO列表
     */
    @Override
    public List<MessageQueryResponseDTO> messageQuery(MessageQueryRequestDTO requestParam) {
        //构件查询条件
        LambdaQueryWrapper<SendRecordDO> queryWrapper = Wrappers.lambdaQuery(SendRecordDO.class)
                .eq(StrUtil.isNotBlank(requestParam.getReceiver()), SendRecordDO::getReceiver, requestParam.getReceiver())
                .eq(StrUtil.isNotBlank(requestParam.getMsgId()), SendRecordDO::getMsgId, requestParam.getMsgId())
                .between(ObjectUtil.isAllNotEmpty(requestParam.getSendStartTime(), requestParam.getSendEndTime()), SendRecordDO::getSendTime, requestParam.getSendStartTime(), requestParam.getSendEndTime());

        //执行消息查询并将结果转换为消息响应DTO列表
        List<MessageQueryResponseDTO> result = BeanUtil.convert(sendRecordMapper.selectList(queryWrapper), MessageQueryResponseDTO.class);
        return result;
    }
}
