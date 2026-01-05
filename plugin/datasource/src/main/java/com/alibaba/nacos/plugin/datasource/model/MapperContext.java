/*
 * Copyright 1999-2022 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.plugin.datasource.model;

import com.alibaba.nacos.plugin.datasource.constants.FieldConstant;

import java.util.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Unified input parameters of the Mapper class.
 *
 * @author hyx
 **/

public class MapperContext {
    
    private final Map<String, Object> whereParamMap;
    
    private final Map<String, Object> updateParamMap;
    
    private final Map<String, String> contextParamMap;
    
    private int startRow;
    
    private int pageSize;
    
    public MapperContext() {
        this.whereParamMap = new HashMap<>();
        this.updateParamMap = new HashMap<>();
        this.contextParamMap = new HashMap<>();
    }
    
    public MapperContext(int startRow, int pageSize) {
        this();
        this.startRow = startRow;
        this.pageSize = pageSize;
    }
    
    /**
     * Returns the value to which the key is mapped, it will return the WHERE parameter in the SQL statement.
     *
     * @param key The key whose associated value is to be returned
     * @return The value to which the key is mapped
     */
    public Object getWhereParameter(String key) {
        if (FieldConstant.START_TIME.equals(key) || FieldConstant.END_TIME.equals(key)
                || FieldConstant.GMT_CREATE.equals(key)) {
            return tryConvertToTimestamp(key);
        }
        return whereParamMap.get(key);
    }

    private Timestamp tryConvertToTimestamp(String key) {
        Object time = whereParamMap.get(key);
        if (time == null) {
            return null;
        }
        if (time instanceof Timestamp) {
            return (Timestamp) time;
        }
        else {
            Timestamp result;
            // 1. 处理 Java 8 LocalDateTime 系列 (最常见)
            if (time instanceof LocalDateTime) {
                result = Timestamp.valueOf((LocalDateTime) time);
            } else if (time instanceof LocalDate) {
                // 只有日期时，自动补全为当天的 00:00:00
                result = Timestamp.valueOf(((LocalDate) time).atStartOfDay());
            } else if (time instanceof OffsetDateTime) {
                result = Timestamp.from(((OffsetDateTime) time).toInstant());
            } else if (time instanceof ZonedDateTime) {
                result = Timestamp.from(((ZonedDateTime) time).toInstant());
            }
            // 2. 处理 传统 java.util.Date 系列
            else if (time instanceof Date) {
                result = new Timestamp(((Date) time).getTime());
            }
            // 3. 处理 数值型 (毫秒时间戳)
            else if (time instanceof Long) {
                result = new Timestamp((Long) time);
            }
            // 4. 处理 字符串型 (最复杂，需考虑多种格式)
            else if (time instanceof String) {
                String strTime = ((String) time).trim().replace("T", " ");
                try {
                    if (strTime.length() == 10) { // yyyy-MM-dd
                        strTime += " 00:00:00";
                    }
                    // 注意：Timestamp.valueOf 支持 yyyy-MM-dd HH:mm:ss.fffffffff 格式
                    result = Timestamp.valueOf(strTime);
                } catch (IllegalArgumentException e) {
                    // 如果以上都失败，可以尝试使用标准 ISO 格式解析
                    try {
                        result = Timestamp.from(LocalDateTime.parse(strTime.replace(" ", "T"))
                                .atZone(java.time.ZoneId.systemDefault()).toInstant());
                    } catch (Exception ex) {
                        throw new RuntimeException("无法解析时间字符串: " + strTime, ex);
                    }
                }
            } else {
                throw new RuntimeException("无法解析为Timestamp类型: " + time.getClass().getName() + "，字符串值为：" + time);
            }
            return result;
        }
    }

    /**
     * Associates the value with the key in this map, it will contain the WHERE parameter in the SQL statement.
     *
     * @param key   Key with which the value is to be associated
     * @param value Value to be associated with the specified key
     */
    public void putWhereParameter(String key, Object value) {
        this.whereParamMap.put(key, value);
    }
    
    /**
     * Returns the value to which the key is mapped, it will return the context param.
     *
     * @param key The key whose associated value is to be returned
     * @return The value to which the key is mapped
     */
    public String getContextParameter(String key) {
        return contextParamMap.get(key);
    }
    
    /**
     * Associates the value with the key in this map, it will contain the context parameter.
     *
     * @param key   Key with which the value is to be associated
     * @param value Value to be associated with the specified key
     */
    public void putContextParameter(String key, String value) {
        this.contextParamMap.put(key, value);
    }
    
    /**
     * Returns the value to which the key is mapped, it will return the UPDATE parameter in the SQL statement.
     *
     * @param key The key whose associated value is to be returned
     * @return The value to which the key is mapped
     */
    public Object getUpdateParameter(String key) {
        return updateParamMap.get(key);
    }
    
    /**
     * Associates the value with the key in this map, it will contain the UPDATE parameter in the SQL statement.
     *
     * @param key   Key with which the value is to be associated
     * @param value Value to be associated with the specified key
     */
    public void putUpdateParameter(String key, Object value) {
        this.updateParamMap.put(key, value);
    }
    
    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }
    
    @Override
    public int hashCode() {
        return super.hashCode();
    }
    
    @Override
    public String toString() {
        return "MapperContext{" + "whereParamMap=" + whereParamMap + '}';
    }
    
    public int getStartRow() {
        return startRow;
    }
    
    public void setStartRow(int startRow) {
        this.startRow = startRow;
    }
    
    public int getPageSize() {
        return pageSize;
    }
    
    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }
}
