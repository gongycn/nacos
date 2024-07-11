package com.alibaba.nacos.plugin.datasource.impl.oracle;

import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.enums.oracle.TrustedOracleFunctionEnum;
import com.alibaba.nacos.plugin.datasource.mapper.AbstractMapper;

import java.util.List;

/**
 * 增加分页查询基础构造SQL
 * 并且处理了tenant_id作为条件时为空字符串的问题
 */
public abstract class AbstractMapperByOracle extends AbstractMapper {

	public static final String TENANT_NULL = "!null!";
	@Override
	public String getDataSource() {
		return DataSourceConstant.ORACLE;
	}

	public String buildPaginationSql(String originalSql, int startRow, int pageSize) {
		String innerRowNumTable = "SELECT TMP.*, ROWNUM ROW_ID FROM ( " + originalSql
				+ " ) TMP WHERE ROWNUM <=" + (startRow + pageSize);
		return "SELECT * FROM ( " + innerRowNumTable + " ) WHERE ROW_ID > " + startRow;
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */

	private static void buildWhereCondition(String whereKey, final StringBuilder sql) {
		String whereValue = " ? ";
		if (whereKey != null && whereKey.toLowerCase().contains("tenant_id")) {
			whereKey = " nvl(" + whereKey + ",'" + TENANT_NULL + "')";
			whereValue = " nvl(?, '" + TENANT_NULL + "') ";
		}
		sql.append(whereKey).append(" ").append("=").append(whereValue);
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 * @param where
	 * @param sql
	 */
	protected void appendWhereClause(List<String> where, StringBuilder sql) {
		sql.append("WHERE ");
		for (int i = 0; i < where.size(); i++) {
			buildWhereCondition(where.get(i), sql);
			if (i != where.size() - 1) {
				sql.append(" AND ");
			}
		}
	}

	@Override
	public String getFunction(String functionName) {
		return TrustedOracleFunctionEnum.getFunctionByName(functionName);
	}
}
