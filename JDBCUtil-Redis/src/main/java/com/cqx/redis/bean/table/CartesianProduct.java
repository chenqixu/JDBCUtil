package com.cqx.redis.bean.table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 笛卡尔积
 *
 * @author chenqixu
 */
public class CartesianProduct {

    private static final Logger logger = LoggerFactory.getLogger(CartesianProduct.class);
    private List<String> cartesianProductList = new ArrayList<>();
    private BlockingQueue<List<String>> blockingQueue = new LinkedBlockingQueue<>();

    public void addQuery(List<String> list) {
        try {
            blockingQueue.put(list);
        } catch (InterruptedException e) {
            logger.error("addQuery异常，" + e.getMessage(), e);
        }
    }

    public void dealCartesianProduct(List<String> resultList) {
        List<String> tmpList;
        while ((tmpList = blockingQueue.poll()) != null) {
            cartesianProductList = addList(resultList, tmpList);
            dealCartesianProduct(cartesianProductList);
        }
    }

    private List<String> addList(List<String> list1, List<String> list2) {
        List<String> result = new ArrayList<>();
        for (String str1 : list1) {
            for (String str2 : list2) {
                result.add(str1 + str2);
            }
        }
        return result;
    }

    public List<String> getCartesianProductList() {
        return cartesianProductList;
    }
}
