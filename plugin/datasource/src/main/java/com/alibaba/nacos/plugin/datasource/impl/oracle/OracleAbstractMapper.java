package com.alibaba.nacos.plugin.datasource.impl.oracle;

import com.alibaba.nacos.plugin.datasource.mapper.AbstractMapper;

import java.util.List;

/**
 * 增加分页查询基础构造SQL
 * 并且处理了tenant_id作为条件时为空字符串的问题
 */
public abstract class OracleAbstractMapper extends AbstractMapper {

	public static final String TENANT_NULL = "!null!";

	public String buildPaginationSql(String originalSql, int startRow, int pageSize) {
//		return "SELECT * FROM ( SELECT TMP2.* FROM (SELECT TMP.*, ROWNUM ROW_ID FROM ( " + originalSql
//				+ " ) TMP) TMP2 WHERE ROWNUM <=" + (startRow + pageSize) + ") WHERE ROW_ID > " + startRow;
		String innerRowNumTable = "SELECT TMP.*, ROWNUM ROW_ID FROM ( " + originalSql
				+ " ) TMP WHERE ROWNUM <=" + (startRow + pageSize);
		return "SELECT * FROM ( " + innerRowNumTable + " ) WHERE ROW_ID > " + startRow;
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	@Override
	public String select(List<String> columns, List<String> where) {
		StringBuilder sql = new StringBuilder();
		String method = "SELECT ";
		sql.append(method);
		for (int i = 0; i < columns.size(); i++) {
			sql.append(columns.get(i));
			if (i == columns.size() - 1) {
				sql.append(" ");
			} else {
				sql.append(",");
			}
		}
		sql.append("FROM ");
		sql.append(getTableName());
		sql.append(" ");

		if (where.size() == 0) {
			return sql.toString();
		}

		sql.append("WHERE ");
		for (int i = 0; i < where.size(); i++) {
			buildWhereCondition(where.get(i), sql);
			if (i != where.size() - 1) {
				sql.append(" AND ");
			}
		}
		return sql.toString();
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	@Override
	public String update(List<String> columns, List<String> where) {
		StringBuilder sql = new StringBuilder();
		String method = "UPDATE ";
		sql.append(method);
		sql.append(getTableName()).append(" ").append("SET ");

		for (int i = 0; i < columns.size(); i++) {
			sql.append(columns.get(i)).append(" = ").append("?");
			if (i != columns.size() - 1) {
				sql.append(",");
			}
		}

		if (where.size() == 0) {
			return sql.toString();
		}

		sql.append(" WHERE ");

		for (int i = 0; i < where.size(); i++) {
			buildWhereCondition(where.get(i), sql);
			if (i != where.size() - 1) {
				sql.append(" AND ");
			}
		}
		return sql.toString();
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	@Override
	public String delete(List<String> params) {
		StringBuilder sql = new StringBuilder();
		String method = "DELETE ";
		sql.append(method).append("FROM ").append(getTableName()).append(" ").append("WHERE ");
		for (int i = 0; i < params.size(); i++) {
			String whereKey = params.get(i);
			buildWhereCondition(whereKey, sql);
			if (i != params.size() - 1) {
				sql.append("AND ");
			}
		}

		return sql.toString();
	}

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
	 */
	@Override
	public String count(List<String> where) {
		StringBuilder sql = new StringBuilder();
		String method = "SELECT ";
		sql.append(method);
		sql.append("COUNT(*) FROM ");
		sql.append(getTableName());
		sql.append(" ");

		if (null == where || where.size() == 0) {
			return sql.toString();
		}

		sql.append("WHERE ");
		for (int i = 0; i < where.size(); i++) {
			buildWhereCondition(where.get(i), sql);
			if (i != where.size() - 1) {
				sql.append(" AND ");
			}
		}
		return sql.toString();
	}
}
