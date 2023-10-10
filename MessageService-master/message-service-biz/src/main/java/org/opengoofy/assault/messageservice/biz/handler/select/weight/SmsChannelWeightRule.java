package org.opengoofy.assault.messageservice.biz.handler.select.weight;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opengoofy.assault.messageservice.biz.config.SmsBaseRuleWeightConfig;
import org.opengoofy.assault.messageservice.biz.config.SmsGeneralRuleWeightConfig;
import org.opengoofy.assault.messageservice.biz.config.SmsMarketingRuleWeightConfig;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.opengoofy.assault.messageservice.biz.common.MessageTypeEnum.SMS_MARKETING_MESSAGE;

/**
 * 短信渠道权重规则
 *
 * @author chen.ma
 * @github https://github.com/opengoofy
 */
@Slf4j
@Component
@RequiredArgsConstructor
public final class SmsChannelWeightRule {
    // 注入短信通用规则的权重配置和营销短信规则的权重配置
    private final SmsGeneralRuleWeightConfig smsGeneralRuleWeightConfig;
    private final SmsMarketingRuleWeightConfig smsMarketingRuleWeightConfig;
    
    /**
     * 根据权重选择短信消息发送渠道方
     *
     * @param msgType    消息类型
     * @param channelIds 渠道 Ids
     * @return 本次发送渠道商
     */
    public String choose(Integer msgType, List<String> channelIds) {

        // 初始化结果渠道商
        String resultChannelId = null;

        // 根据消息类型选择相应的短信规则配置
        SmsBaseRuleWeightConfig ruleWeightConfig = Objects.equals(msgType, SMS_MARKETING_MESSAGE.getType())
                ? smsMarketingRuleWeightConfig : smsGeneralRuleWeightConfig;
        try {
            // 通过当前短信附带短信运营商集合与配置中心运营商权重进行取交集
            List<SmsBaseRuleWeightConfig.Weight> weightList = buildWeightList(ruleWeightConfig, channelIds);
            // 通过权重计算应该采用哪个短信运营商下标
            int idx = getWeightIndex(weightList);
            // 通过下标获取对应渠道商 ID
            resultChannelId = getTemplateConfig(idx, weightList, channelIds);
        } catch (Exception ex) {
            log.error("短信渠道商计算权重算法错误", ex);
        }
        return resultChannelId;
    }

    // 构建符合当前短信可选运营商集合的权重列表
    private List<SmsBaseRuleWeightConfig.Weight> buildWeightList(SmsBaseRuleWeightConfig smsBaseRuleWeightConfig, List<String> channelIds) {
        // 获取配置中心权重集合 & 当前短信可选择供应商数据交集
        List<SmsBaseRuleWeightConfig.Weight> weightList = smsBaseRuleWeightConfig.getWeightList().stream()
                .filter(each -> channelIds.contains(each.getChannelId()))
                .collect(Collectors.toList());
        return weightList;
    }

    // 根据权重计算应该采用哪个短信运营商的下标
    private Integer getWeightIndex(List<SmsBaseRuleWeightConfig.Weight> weightList) {
        int idx = -1;
        // 统计所有权重数量合
        int weightSum = getWeightSum(weightList);
        double tempOne = 0;
        double tempTwo = 0;
        double randomNumber = Math.random();
        for (int i = 0; i < weightList.size(); i++) {
            // 获取权重
            String weight = weightList.get(i).getWeight();
            tempTwo += Double.parseDouble(weight) / weightSum;
            if (i == 0) {
                tempOne = 0;
            } else {
                weight = weightList.get(i - 1).getWeight();
                tempOne += Double.parseDouble(weight) / weightSum;
            }
            if (randomNumber >= tempOne && randomNumber <= tempTwo) {
                idx = i;
                break;
            }
        }
        return idx;
    }

    // 计算权重列表中权重的总和
    private Integer getWeightSum(List<SmsBaseRuleWeightConfig.Weight> weightList) {
        return weightList.stream().map(each -> each.getWeight()).mapToInt(Integer::new).sum();
    }
    
    private String getTemplateConfig(int idx, List<SmsBaseRuleWeightConfig.Weight> weightList, List<String> channelIds) {
        // 获取当前已选择供应商的标识
        String channelId = weightList.get(idx).getChannelId();
        // 根据供应商标识从当前短信可选择供应商列表中获取具体数据
        Optional<String> templateConfig = channelIds.stream()
                .filter(each -> Objects.equals(each, channelId))
                .findFirst();
        // 这里直接返回空，由上层服务决定异常如何抛出及信息打印
        return templateConfig.orElse(null);
    }
}

/*
根据权重，使得权重较大的渠道商被选中的概率更大，从而实现了按照权重选择渠道商的功能。
 */