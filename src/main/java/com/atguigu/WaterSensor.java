package com.atguigu;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data    //get set toString 方法
@NoArgsConstructor  //无参构造器
@AllArgsConstructor //带所有参数的构造器

public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;


}
