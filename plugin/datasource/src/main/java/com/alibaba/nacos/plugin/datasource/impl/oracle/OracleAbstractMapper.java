package com.alibaba.nacos.plugin.datasource.impl.oracle;

import com.alibaba.nacos.plugin.datasource.mapper.AbstractMapper;

/**
 * 增加分页查询基础构造SQL
 */
public abstract class OracleAbstractMapper extends AbstractMapper {

	public String buildPaginationSql(String originalSql, int startRow, int pageSize) {
//		return "SELECT * FROM ( SELECT TMP2.* FROM (SELECT TMP.*, ROWNUM ROW_ID FROM ( " + originalSql
//				+ " ) TMP) TMP2 WHERE ROWNUM <=" + (startRow + pageSize) + ") WHERE ROW_ID > " + startRow;
		String innerRowNumTable = "SELECT TMP.*, ROWNUM ROW_ID FROM ( " + originalSql
				+ " ) TMP WHERE ROWNUM <=" + (startRow + pageSize);
		return "SELECT * FROM ( " + innerRowNumTable + " ) WHERE ROW_ID > " + startRow;
	}
}
