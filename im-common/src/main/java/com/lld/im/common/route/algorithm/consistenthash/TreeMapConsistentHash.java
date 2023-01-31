package com.lld.im.common.route.algorithm.consistenthash;

import com.lld.im.common.enums.UserErrorCode;
import com.lld.im.common.exception.ApplicationException;

import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @description:
 * @author: lld
 * @version: 1.0
 */
public class TreeMapConsistentHash extends AbstractConsistentHash {

    private TreeMap<Long, String> treeMap = new TreeMap<>();

    private static final int NODE_SIZE = 2;

    @Override
    protected void add(long key, String value) {
        for (int i = 0; i < NODE_SIZE; i++) {
            treeMap.put(super.hash("node" + key + i), value);
        }
        treeMap.put(key, value);
    }


    @Override
    protected String getFirstNodeValue(String value) {

        Long hashkey = super.hash(value);
        //此方法返回此映射，其键大于或等于hash的部分视图。
        SortedMap<Long, String> last = treeMap.tailMap(hashkey);
        if (!last.isEmpty()) {
            // 该方法返回此映射第一个（最低）键。
            return last.get(last.firstKey());
        }

        if (treeMap.size() == 0) {
            throw new ApplicationException(UserErrorCode.SERVER_NOT_AVAILABLE);
        }
        //此方法返回的最小键在这个映射中的键- 值映射关系，如果映射为空，返回null。
        return treeMap.firstEntry().getValue();
    }

    @Override
    protected void processBefore() {
        treeMap.clear();
    }
}
