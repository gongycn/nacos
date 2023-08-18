package com.alibaba.nacos.plugin.datasource.impl.oracle;

import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoMapper;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public class ConfigInfoMapperByOracle extends OracleAbstractMapper implements ConfigInfoMapper {

	private static final String DATA_ID = "dataId";

	private static final String GROUP = "group";

	private static final String APP_NAME = "appName";

	private static final String CONTENT = "content";

	private static final String TENANT = "tenant";

	@Override
	public String findConfigInfoByAppFetchRows(int startRow, int pageSize) {
		String sql = "SELECT id,data_id,group_id,tenant_id,app_name,content FROM config_info"
				+ " WHERE nvl(tenant_id, '" + TENANT_NULL + "') LIKE nvl(?, '" + TENANT_NULL + "') AND app_name= ?";
		return buildPaginationSql(sql, startRow, pageSize);
	}

	@Override
	public String getTenantIdList(int startRow, int pageSize) {
		String sql = "SELECT tenant_id FROM config_info WHERE tenant_id IS NOT NULL GROUP BY tenant_id ";
		return buildPaginationSql(sql, startRow, pageSize);
	}

	@Override
	public String getGroupIdList(int startRow, int pageSize) {
		String sql = "SELECT group_id FROM config_info WHERE tenant_id IS NULL GROUP BY group_id ";
		return buildPaginationSql(sql, startRow, pageSize);
	}

	@Override
	public String findAllConfigKey(int startRow, int pageSize) {
		String sql = " SELECT id,data_id,group_id,app_name FROM config_info WHERE nvl(tenant_id, '" + TENANT_NULL + "') LIKE nvl(?, '" + TENANT_NULL + "') ORDER BY id ";
		return buildPaginationSql(sql, startRow, pageSize);
	}

	@Override
	public String findAllConfigInfoBaseFetchRows(int startRow, int pageSize) {
		String sql = "SELECT id,data_id,group_id,content,md5 FROM  config_info  ORDER BY id  ";
		return buildPaginationSql(sql, startRow, pageSize);
	}

	@Override
	public String findAllConfigInfoFragment(int startRow, int pageSize) {
		String sql = "SELECT id,data_id,group_id,tenant_id,app_name,content,md5,gmt_modified,type,encrypted_data_key "
				+ "FROM config_info WHERE id > ? ORDER BY id ASC ";
		return buildPaginationSql(sql, startRow, pageSize);
	}

	@Override
	public String findChangeConfigFetchRows(Map<String, String> params, final Timestamp startTime,
			final Timestamp endTime, int startRow, int pageSize, long lastMaxId) {
		final String tenant = params.get(TENANT);
		final String dataId = params.get(DATA_ID);
		final String group = params.get(GROUP);
		final String appName = params.get(APP_NAME);
		final String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
		final String sqlFetchRows = "SELECT id,data_id,group_id,tenant_id,app_name,content,type,md5,gmt_modified FROM config_info WHERE ";
		String where = " 1=1 ";
		if (!StringUtils.isBlank(dataId)) {
			where += " AND data_id LIKE ? ";
		}
		if (!StringUtils.isBlank(group)) {
			where += " AND group_id LIKE ? ";
		}

		if (!StringUtils.isBlank(tenantTmp)) {
			where += " AND tenant_id = ? ";
		}

		if (!StringUtils.isBlank(appName)) {
			where += " AND app_name = ? ";
		}
		if (startTime != null) {
			where += " AND gmt_modified >=? ";
		}
		if (endTime != null) {
			where += " AND gmt_modified <=? ";
		}

		String sql = sqlFetchRows + where + " AND id > " + lastMaxId + " ORDER BY id ASC";
		return buildPaginationSql(sql, 0, pageSize);
	}

	@Override
	public String listGroupKeyMd5ByPageFetchRows(int startRow, int pageSize) {
		String sql = "SELECT id,data_id,group_id,tenant_id,app_name,md5,type,gmt_modified,encrypted_data_key from config_info  ORDER BY id ";
		return buildPaginationSql(sql, startRow, pageSize);
	}


	@Override
	public String findConfigInfoBaseLikeFetchRows(Map<String, String> params, int startRow, int pageSize) {
		final String sqlFetchRows = "SELECT id,data_id,group_id,tenant_id,content FROM config_info WHERE ";
		String where = " 1=1 AND tenant_id IS NULL ";
		if (!StringUtils.isBlank(params.get(DATA_ID))) {
			where += " AND data_id LIKE ? ";
		}
		if (!StringUtils.isBlank(params.get(GROUP))) {
			where += " AND group_id LIKE ";
		}
		if (!StringUtils.isBlank(params.get(CONTENT))) {
			where += " AND content LIKE ? ";
		}
		String sql = sqlFetchRows + where;
		return buildPaginationSql(sql, startRow, pageSize);
	}

	@Override
	public String findConfigInfo4PageFetchRows(Map<String, String> params, int startRow, int pageSize) {
		final String appName = params.get(APP_NAME);
		final String dataId = params.get(DATA_ID);
		final String group = params.get(GROUP);
		final String sql = "SELECT id,data_id,group_id,tenant_id,app_name,content,type,encrypted_data_key FROM config_info";
		StringBuilder where = new StringBuilder(" WHERE ");
		where.append(" tenant_id= ? ");
		if (StringUtils.isNotBlank(dataId)) {
			where.append(" AND data_id=? ");
		}
		if (StringUtils.isNotBlank(group)) {
			where.append(" AND group_id=? ");
		}
		if (StringUtils.isNotBlank(appName)) {
			where.append(" AND app_name=? ");
		}
		return buildPaginationSql(sql + where, startRow, pageSize);
	}

	@Override
	public String findConfigInfoBaseByGroupFetchRows(int startRow, int pageSize) {
		String sql = "SELECT id,data_id,group_id,content FROM config_info WHERE group_id=? AND nvl(tenant_id, '" + TENANT_NULL + "') = nvl(?, '" + TENANT_NULL + "') ";
		return buildPaginationSql(sql, startRow, pageSize);
	}

	@Override
	public String findConfigInfoLike4PageFetchRows(Map<String, String> params, int startRow, int pageSize) {
		String dataId = params.get(DATA_ID);
		String group = params.get(GROUP);
		final String appName = params.get(APP_NAME);
		final String content = params.get(CONTENT);
		final String sqlFetchRows = "SELECT id,data_id,group_id,tenant_id,app_name,content,encrypted_data_key FROM config_info";
		StringBuilder where = new StringBuilder(" WHERE ");
		where.append(" nvl(tenant_id, '" + TENANT_NULL + "') LIKE nvl(?, '" + TENANT_NULL + "') ");
		if (!StringUtils.isBlank(dataId)) {
			where.append(" AND data_id LIKE ? ");
		}
		if (!StringUtils.isBlank(group)) {
			where.append(" AND group_id LIKE ? ");
		}
		if (!StringUtils.isBlank(appName)) {
			where.append(" AND app_name = ? ");
		}
		if (!StringUtils.isBlank(content)) {
			where.append(" AND content LIKE ? ");
		}
		return buildPaginationSql(sqlFetchRows + where, startRow, pageSize);
	}

	@Override
	public String findAllConfigInfoFetchRows(int startRow, int pageSize) {
		String sql = "SELECT id,data_id,group_id,tenant_id,app_name,content,md5 "
				+ " FROM  config_info WHERE nvl(tenant_id, '" + TENANT_NULL + "') LIKE nvl(?, '" + TENANT_NULL + "') ORDER BY id ";
		return buildPaginationSql(sql, startRow, pageSize);
	}

	@Override
	public String getDataSource() {
		return DataSourceConstant.ORACLE;
	}


	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public String findConfigInfoByAppCountRows() {
		return "SELECT count(*) FROM config_info WHERE nvl(tenant_id, '" + TENANT_NULL + "') LIKE nvl(?, '" + TENANT_NULL + "') AND app_name = ?";
	}
	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public String configInfoLikeTenantCount() {
		return "SELECT count(*) FROM config_info WHERE nvl(tenant_id, '" + TENANT_NULL + "') LIKE nvl(?, '" + TENANT_NULL + "')";
	}
	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	@Override
	public String findConfigInfoBaseLikeCountRows(Map<String, String> params) {
		final String sqlCountRows = "SELECT count(*) FROM config_info WHERE ";
		String where = " 1=1 AND tenant_id IS NULL ";

		if (!StringUtils.isBlank(params.get(DATA_ID))) {
			where += " AND data_id LIKE ? ";
		}
		if (!StringUtils.isBlank(params.get(GROUP))) {
			where += " AND group_id LIKE ";
		}
		if (!StringUtils.isBlank(params.get(CONTENT))) {
			where += " AND content LIKE ? ";
		}
		return sqlCountRows + where;
	}
	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public String findAllConfigInfo4Export(List<Long> ids, Map<String, String> params) {
		String sql = "SELECT id,data_id,group_id,tenant_id,app_name,content,type,md5,gmt_create,gmt_modified,"
				+ "src_user,src_ip,c_desc,c_use,effect,c_schema,encrypted_data_key FROM config_info";
		StringBuilder where = new StringBuilder(" WHERE ");
		if (!CollectionUtils.isEmpty(ids)) {
			where.append(" id IN (");
			for (int i = 0; i < ids.size(); i++) {
				if (i != 0) {
					where.append(", ");
				}
				where.append('?');
			}
			where.append(") ");
		} else {
			where.append(" nvl(tenant_id, '" + TENANT_NULL + "') = nvl(?, '" + TENANT_NULL + "') ");
			if (StringUtils.isNotBlank(params.get(DATA_ID))) {
				where.append(" AND data_id LIKE ? ");
			}
			if (StringUtils.isNotBlank(params.get(GROUP))) {
				where.append(" AND group_id= ? ");
			}
			if (StringUtils.isNotBlank(params.get(APP_NAME))) {
				where.append(" AND app_name= ? ");
			}
		}
		return sql + where;
	}
	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public String findConfigInfo4PageCountRows(Map<String, String> params) {
		final String appName = params.get(APP_NAME);
		final String dataId = params.get(DATA_ID);
		final String group = params.get(GROUP);
		final String content = params.get(CONTENT);
		final String sqlCount = "SELECT count(*) FROM config_info";
		StringBuilder where = new StringBuilder(" WHERE ");
		where.append(" nvl(tenant_id, '" + TENANT_NULL + "') = nvl(?, '" + TENANT_NULL + "') ");
		if (StringUtils.isNotBlank(dataId)) {
			where.append(" AND data_id=? ");
		}
		if (StringUtils.isNotBlank(group)) {
			where.append(" AND group_id=? ");
		}
		if (StringUtils.isNotBlank(appName)) {
			where.append(" AND app_name=? ");
		}
		if (!StringUtils.isBlank(content)) {
			where.append(" AND content LIKE ? ");
		}
		return sqlCount + where;
	}
	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public String findConfigInfoLike4PageCountRows(Map<String, String> params) {
		final String appName = params.get("appName");
		final String content = params.get("content");
		final String dataId = params.get("dataId");
		final String group = params.get(GROUP);
		final String sqlCountRows = "SELECT count(*) FROM config_info";
		StringBuilder where = new StringBuilder(" WHERE ");
		where.append(" nvl(tenant_id, '" + TENANT_NULL + "') LIKE nvl(?, '" + TENANT_NULL + "') ");
		if (!StringUtils.isBlank(dataId)) {
			where.append(" AND data_id LIKE ? ");
		}
		if (!StringUtils.isBlank(group)) {
			where.append(" AND group_id LIKE ? ");
		}
		if (!StringUtils.isBlank(appName)) {
			where.append(" AND app_name = ? ");
		}
		if (!StringUtils.isBlank(content)) {
			where.append(" AND content LIKE ? ");
		}
		return sqlCountRows + where;
	}
	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public String updateConfigInfoAtomicCas() {
		return "UPDATE config_info SET "
				+ "content=?, md5 = ?, src_ip=?,src_user=?,gmt_modified=?, app_name=?,c_desc=?,c_use=?,effect=?,type=?,c_schema=? "
				+ "WHERE data_id=? AND group_id=? AND nvl(tenant_id, '" + TENANT_NULL + "') = nvl(?, '" + TENANT_NULL + "') AND (md5=? OR md5 IS NULL OR md5='')";
	}
}
