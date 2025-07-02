package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public List<ShopType> queryTypeList() {
        // 1.从redis中查找数据
        String key = RedisConstants.CACHE_SHOP_KEY + "shopType";
        List<String> list = stringRedisTemplate.opsForList().range(key, 0, -1);
        // 2.找到后返回
        if (list != null && list.size() > 0) {
            List<ShopType> shopTypeList = new ArrayList<>();
            for (String s : list) {
                ShopType shopType = JSONUtil.toBean(s, ShopType.class);
                shopTypeList.add(shopType);
            }
            return shopTypeList;
        }

        // 3.找不到去mysql查
        List<ShopType> typeList = query().orderByAsc("sort").list();
        // 4.保存到redis
        List<String> list1 = new ArrayList<>();
        for (ShopType shopType : typeList) {
            list1.add(JSONUtil.toJsonStr(shopType));
        }
        stringRedisTemplate.opsForList().leftPushAll(key, list1);
        // 5.返回数据
        return typeList;
    }
}
