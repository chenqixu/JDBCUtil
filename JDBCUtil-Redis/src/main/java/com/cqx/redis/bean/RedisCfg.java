package com.cqx.redis.bean;

import com.cqx.redis.client.RedisFactory;

/**
 * RedisCfg
 *
 * @author chenqixu
 */
public class RedisCfg {
    private int mode_type = RedisFactory.SINGLE_MODE_TYPE;
    private String ip;
    private int port;
    private String ip_ports;

    public static RedisCfg builder() {
        return new RedisCfg();
    }

    public int getMode_type() {
        return mode_type;
    }

    public RedisCfg setMode_type(int mode_type) {
        this.mode_type = mode_type;
        return this;
    }

    public String getIp() {
        return ip;
    }

    public RedisCfg setIp(String ip) {
        this.ip = ip;
        return this;
    }

    public int getPort() {
        return port;
    }

    public RedisCfg setPort(int port) {
        this.port = port;
        return this;
    }

    public String getIp_ports() {
        return ip_ports;
    }

    public RedisCfg setIp_ports(String ip_ports) {
        this.ip_ports = ip_ports;
        return this;
    }
}
