package com.cqx.redis.utils;

/**
 * StartsWithResult
 *
 * @author chenqixu
 */
public class StartsWithResult {
    private boolean isStartsWith;
    private String str;

    public StartsWithResult() {
    }

    public StartsWithResult(boolean isStartsWith, String str) {
        this.isStartsWith = isStartsWith;
        this.str = str;
    }

    public boolean isStartsWith() {
        return isStartsWith;
    }

    public void setStartsWith(boolean startsWith) {
        isStartsWith = startsWith;
    }

    public String getStr() {
        return str;
    }

    public void setStr(String str) {
        this.str = str;
    }
}
