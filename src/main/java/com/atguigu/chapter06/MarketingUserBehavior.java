package com.atguigu.chapter06;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author atguigu.cn
 * @Date 2020/12/10 21:41
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MarketingUserBehavior {
    private Long userId;
    private String behavior;
    private String channel;
    private Long timestamp;
}
