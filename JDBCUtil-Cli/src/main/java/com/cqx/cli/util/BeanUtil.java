package com.cqx.cli.util;

import com.cqx.cli.bean.CmdBean;
import com.cqx.redis.bean.RedisCfg;

/**
 * BeanUtil
 *
 * @author chenqixu
 */
public class BeanUtil {

    public static CmdBean rediscfgTocmd(RedisCfg redisCfg) {
        return CmdBean.newbuilder()
                .setUsername("")
                .setPassword("")
                .setType("redis")
                .setDns(redisCfg.getIp_ports());
    }
}
