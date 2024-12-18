package com.alibaba.nacos.plugin.datasource.impl.oracle;

import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.plugin.datasource.constants.FieldConstant;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ConfigInfoMapperByOracle extends AbstractMapperByOracle implements ConfigInfoMapper {
	@Override
	public MapperResult findConfigInfoByAppFetchRows(MapperContext context) {
		int startRow = context.getStartRow();
		int pageSize = context.getPageSize();
		final String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);
		final String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
		List<Object> paramList = new ArrayList<>();
		String tenantIdQuery = "= ?";
		if (StringUtils.isEmpty(tenantId)) {
			tenantIdQuery = "is null";
		} else {
			paramList.add(tenantId);
		}
		paramList.add(appName);
		String sql = "SELECT id,data_id,group_id,tenant_id,app_name,content FROM config_info"
				+ " WHERE tenant_id " + tenantIdQuery + " AND app_name= ?";
		sql = buildPaginationSql(sql, startRow, pageSize);
		return new MapperResult(sql, paramList);
	}

	@Override
	public MapperResult getTenantIdList(MapperContext context) {
		int startRow = context.getStartRow();
		int pageSize = context.getPageSize();
		String sql = "SELECT tenant_id FROM config_info WHERE tenant_id IS NOT NULL GROUP BY tenant_id ";
		sql = buildPaginationSql(sql, startRow, pageSize);
		return new MapperResult(sql, Collections.emptyList());
	}

	@Override
	public MapperResult getGroupIdList(MapperContext context) {
		int startRow = context.getStartRow();
		int pageSize = context.getPageSize();
		String sql = "SELECT group_id FROM config_info WHERE tenant_id IS NULL GROUP BY group_id ";
		sql = buildPaginationSql(sql, startRow, pageSize);
		return new MapperResult(sql, Collections.emptyList());
	}

	@Override
	public MapperResult findAllConfigKey(MapperContext context) {
		int startRow = context.getStartRow();
		int pageSize = context.getPageSize();

		String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
		String tenantIdQuery = "= ?";
		List<Object> paramList = new ArrayList<>();
		if (StringUtils.isEmpty(tenantId)) {
			tenantIdQuery = "is null";
		} else {
			paramList.add(tenantId);
		}
		String sql = " SELECT id,data_id,group_id,app_name FROM config_info WHERE tenant_id " + tenantIdQuery + " ORDER BY id ";
		sql = buildPaginationSql(sql, startRow, pageSize);
		return new MapperResult(sql, paramList);
	}

	@Override
	public MapperResult findAllConfigInfoBaseFetchRows(MapperContext context) {
		int startRow = context.getStartRow();
		int pageSize = context.getPageSize();
		String sql = "SELECT id,data_id,group_id,content,md5 FROM  config_info  ORDER BY id  ";
		sql = buildPaginationSql(sql, startRow, pageSize);
		return new MapperResult(sql, Collections.emptyList());
	}

	@Override
	public MapperResult findAllConfigInfoFragment(MapperContext context) {
		int startRow = context.getStartRow();
		int pageSize = context.getPageSize();
		String sql = "SELECT id,data_id,group_id,tenant_id,app_name,content,md5,gmt_modified,type,encrypted_data_key "
				+ "FROM config_info WHERE id > ? ORDER BY id ASC ";
		sql = buildPaginationSql(sql, startRow, pageSize);
		return new MapperResult(sql, CollectionUtils.list(context.getWhereParameter(FieldConstant.ID)));
	}

	@Override
	public MapperResult findChangeConfigFetchRows(MapperContext context) {
		final String tenant = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
		final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
		final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
		final String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);
		final String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
		final Timestamp startTime = (Timestamp) context.getWhereParameter(FieldConstant.START_TIME);
		final Timestamp endTime = (Timestamp) context.getWhereParameter(FieldConstant.END_TIME);
		List<Object> paramList = new ArrayList<>();
		final String sqlFetchRows = "SELECT id,data_id,group_id,tenant_id,app_name,content,type,md5,gmt_modified FROM config_info WHERE ";
		String where = " 1=1 ";
		if (!StringUtils.isBlank(dataId)) {
			where += " AND data_id LIKE ? ";
			paramList.add(dataId);
		}
		if (!StringUtils.isBlank(group)) {
			where += " AND group_id LIKE ? ";
			paramList.add(group);
		}

		if (!StringUtils.isBlank(tenantTmp)) {
			where += " AND tenant_id = ? ";
			paramList.add(tenantTmp);
		}

		if (!StringUtils.isBlank(appName)) {
			where += " AND app_name = ? ";
			paramList.add(appName);
		}
		if (startTime != null) {
			where += " AND gmt_modified >=? ";
			paramList.add(startTime);
		}
		if (endTime != null) {
			where += " AND gmt_modified <=? ";
			paramList.add(endTime);
		}

		String sql = sqlFetchRows + where + " AND id > " + context.getWhereParameter(FieldConstant.LAST_MAX_ID) + " ORDER BY id ASC";
		sql = buildPaginationSql(sql, 0, context.getPageSize());
		return new MapperResult(sql, paramList);
	}

	@Override
	public MapperResult listGroupKeyMd5ByPageFetchRows(MapperContext context) {
		int startRow = context.getStartRow();
		int pageSize = context.getPageSize();
		String sql = "SELECT id,data_id,group_id,tenant_id,app_name,md5,type,gmt_modified,encrypted_data_key from config_info  ORDER BY id ";
		sql = buildPaginationSql(sql, startRow, pageSize);
		return new MapperResult(sql, Collections.emptyList());
	}


	@Override
	public MapperResult findConfigInfoBaseLikeFetchRows(MapperContext context) {
		int startRow = context.getStartRow();
		int pageSize = context.getPageSize();
		final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
		final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
		final String content = (String) context.getWhereParameter(FieldConstant.CONTENT);
		final String sqlFetchRows = "SELECT id,data_id,group_id,tenant_id,content FROM config_info WHERE ";
		String where = " 1=1 AND tenant_id IS NULL ";
		List<Object> paramList = new ArrayList<>();

		if (!StringUtils.isBlank(dataId)) {
			where += " AND data_id LIKE ? ";
			paramList.add(dataId);
		}
		if (!StringUtils.isBlank(group)) {
			where += " AND group_id LIKE ";
			paramList.add(group);
		}
		if (!StringUtils.isBlank(content)) {
			where += " AND content LIKE ? ";
			paramList.add(content);
		}
		String sql = sqlFetchRows + where;
		sql = buildPaginationSql(sql, startRow, pageSize);
		return new MapperResult(sql, paramList);
	}

	@Override
	public MapperResult findConfigInfo4PageFetchRows(MapperContext context) {
		int startRow = context.getStartRow();
		int pageSize = context.getPageSize();
		final String tenant = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
		final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
		final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
		final String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);
		final String content = (String) context.getWhereParameter(FieldConstant.CONTENT);
		List<Object> paramList = new ArrayList<>();
		String sql = "SELECT id,data_id,group_id,tenant_id,app_name,content,type,encrypted_data_key FROM config_info";
		StringBuilder where = new StringBuilder(" WHERE ");

		if (StringUtils.isNotBlank(tenant)) {
			where.append(" tenant_id= ? ");
			paramList.add(tenant);
		} else {
			where.append(" tenant_id is null ");
		}
		if (StringUtils.isNotBlank(dataId)) {
			where.append(" AND data_id=? ");
			paramList.add(dataId);
		}
		if (StringUtils.isNotBlank(group)) {
			where.append(" AND group_id=? ");
			paramList.add(group);
		}
		if (StringUtils.isNotBlank(appName)) {
			where.append(" AND app_name=? ");
			paramList.add(appName);
		}
		if (!StringUtils.isBlank(content)) {
			where.append(" AND content LIKE ? ");
			paramList.add(content);
		}
		sql = buildPaginationSql(sql + where, startRow, pageSize);
		return new MapperResult(sql, paramList);
	}

	@Override
	public MapperResult findConfigInfoBaseByGroupFetchRows(MapperContext context) {
		int startRow = context.getStartRow();
		int pageSize = context.getPageSize();
		String sql = "SELECT id,data_id,group_id,content FROM config_info WHERE group_id=? AND nvl(tenant_id, '" + TENANT_NULL + "') = nvl(?, '" + TENANT_NULL + "') ";

		sql = buildPaginationSql(sql, startRow, pageSize);
		return new MapperResult(sql, Collections.emptyList());
	}

	@Override
	public MapperResult findConfigInfoLike4PageFetchRows(MapperContext context) {
		int startRow = context.getStartRow();
		int pageSize = context.getPageSize();
		final String tenant = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
		final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
		final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
		final String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);
		final String content = (String) context.getWhereParameter(FieldConstant.CONTENT);

		List<Object> paramList = new ArrayList<>();
		String sqlFetchRows = "SELECT id,data_id,group_id,tenant_id,app_name,content,encrypted_data_key FROM config_info";
		StringBuilder where = new StringBuilder(" WHERE ");

		if (StringUtils.isNotBlank(tenant)) {
			where.append(" tenant_id= ? ");
			paramList.add(tenant);
		} else {
			where.append(" tenant_id is null ");
		}
		if (!StringUtils.isBlank(dataId)) {
			where.append(" AND data_id LIKE ? ");
			paramList.add(dataId);

		}
		if (!StringUtils.isBlank(group)) {
			where.append(" AND group_id LIKE ? ");
			paramList.add(group);
		}
		if (!StringUtils.isBlank(appName)) {
			where.append(" AND app_name = ? ");
			paramList.add(appName);
		}
		if (!StringUtils.isBlank(content)) {
			where.append(" AND content LIKE ? ");
			paramList.add(content);
		}
		sqlFetchRows = buildPaginationSql(sqlFetchRows + where, startRow, pageSize);
		return new MapperResult(sqlFetchRows, paramList);
	}

	@Override
	public MapperResult findAllConfigInfoFetchRows(MapperContext context) {
		int startRow = context.getStartRow();
		int pageSize = context.getPageSize();
		String sql = "SELECT id,data_id,group_id,tenant_id,app_name,content,md5 "
				+ " FROM  config_info WHERE nvl(tenant_id, '" + TENANT_NULL + "') LIKE nvl(?, '" + TENANT_NULL + "') ORDER BY id ";
		sql = buildPaginationSql(sql, startRow, pageSize);
		return new MapperResult(sql, Collections.emptyList());
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public MapperResult findConfigInfoByAppCountRows(MapperContext context) {
		String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
		Object appName = context.getWhereParameter(FieldConstant.APP_NAME);
		List<Object> paramList = new ArrayList<>();
		StringBuilder where = new StringBuilder(" WHERE ");
		if (StringUtils.isNotBlank(tenantId)) {
			where.append(" tenant_id LIKE ? ");
			paramList.add(tenantId);
		} else {
			where.append(" tenant_id is null ");
		}
		where.append(" AND app_name = ?");
		paramList.add(appName);
		String sql = "SELECT count(*) FROM config_info";
		return new MapperResult(sql + where, paramList);
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public MapperResult configInfoLikeTenantCount(MapperContext context) {
		String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
		List<Object> paramList = new ArrayList<>();
		StringBuilder where = new StringBuilder(" WHERE ");
		if (StringUtils.isNotBlank(tenantId)) {
			where.append(" tenant_id LIKE ? ");
			paramList.add(tenantId);
		} else {
			where.append(" tenant_id is null ");
		}
		String sql = "SELECT count(*) FROM config_info";
		return new MapperResult(sql + where, paramList);
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	@Override
	public MapperResult findConfigInfoBaseLikeCountRows(MapperContext context) {
		final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
		final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
		final String content = (String) context.getWhereParameter(FieldConstant.CONTENT);

		final List<Object> paramList = new ArrayList<>();
		final String sqlCountRows = "SELECT count(*) FROM config_info WHERE ";
		String where = " 1=1 AND tenant_id is null ";

		if (!StringUtils.isBlank(dataId)) {
			where += " AND data_id LIKE ? ";
			paramList.add(dataId);
		}
		if (!StringUtils.isBlank(group)) {
			where += " AND group_id LIKE ? ";
			paramList.add(group);
		}
		if (!StringUtils.isBlank(content)) {
			where += " AND content LIKE ? ";
			paramList.add(content);
		}
		return new MapperResult(sqlCountRows + where, paramList);
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public MapperResult findAllConfigInfo4Export(MapperContext context) {
		List<Long> ids = (List<Long>) context.getWhereParameter(FieldConstant.IDS);

		String sql = "SELECT id,data_id,group_id,tenant_id,app_name,content,type,md5,gmt_create,gmt_modified,"
				+ "src_user,src_ip,c_desc,c_use,effect,c_schema,encrypted_data_key FROM config_info";
		StringBuilder where = new StringBuilder(" WHERE ");

		List<Object> paramList = new ArrayList<>();
		if (!CollectionUtils.isEmpty(ids)) {
			where.append(" id IN (");
			for (int i = 0; i < ids.size(); i++) {
				if (i != 0) {
					where.append(", ");
				}
				where.append('?');
				paramList.add(ids.get(i));
			}
			where.append(") ");
		} else {
			String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
			if (StringUtils.isNotBlank(tenantId)) {
				where.append(" tenant_id = ? ");
				paramList.add(tenantId);
			} else {

				where.append(" tenant_id is null ");
			}
			String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
			String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
			String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);

			if (StringUtils.isNotBlank(dataId)) {
				where.append(" AND data_id LIKE ? ");
				paramList.add(dataId);
			}
			if (StringUtils.isNotBlank(group)) {
				where.append(" AND group_id= ? ");
				paramList.add(group);
			}
			if (StringUtils.isNotBlank(appName)) {
				where.append(" AND app_name= ? ");
				paramList.add(appName);
			}
		}
		return new MapperResult(sql + where, paramList);
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public MapperResult findConfigInfo4PageCountRows(MapperContext context) {
		final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
		final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
		final String content = (String) context.getWhereParameter(FieldConstant.CONTENT);
		final String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);
		final String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
		final List<Object> paramList = new ArrayList<>();

		final String sqlCount = "SELECT count(*) FROM config_info";
		StringBuilder where = new StringBuilder(" WHERE ");
		if (StringUtils.isNotBlank(tenantId)) {
			where.append(" tenant_id=? ");
			paramList.add(tenantId);
		} else {
			where.append(" tenant_id is null ");
		}
		if (StringUtils.isNotBlank(dataId)) {
			where.append(" AND data_id=? ");
			paramList.add(dataId);
		}
		if (StringUtils.isNotBlank(group)) {
			where.append(" AND group_id=? ");
			paramList.add(group);
		}
		if (StringUtils.isNotBlank(appName)) {
			where.append(" AND app_name=? ");
			paramList.add(appName);
		}
		if (!StringUtils.isBlank(content)) {
			where.append(" AND content LIKE ? ");
			paramList.add(content);
		}
		return new MapperResult(sqlCount + where, paramList);
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public MapperResult findConfigInfoLike4PageCountRows(MapperContext context) {
		final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
		final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
		final String content = (String) context.getWhereParameter(FieldConstant.CONTENT);
		final String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);
		final String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);

		final List<Object> paramList = new ArrayList<>();

		final String sqlCountRows = "SELECT count(*) FROM config_info";
		StringBuilder where = new StringBuilder(" WHERE ");
		if (StringUtils.isNotBlank(tenantId)) {
			where.append(" tenant_id LIKE ? ");
			paramList.add(tenantId);
		} else {
			where.append(" tenant_id is null ");
		}
		if (!StringUtils.isBlank(dataId)) {
			where.append(" AND data_id LIKE ? ");
			paramList.add(dataId);
		}
		if (!StringUtils.isBlank(group)) {
			where.append(" AND group_id LIKE ? ");
			paramList.add(group);
		}
		if (!StringUtils.isBlank(appName)) {
			where.append(" AND app_name = ? ");
			paramList.add(appName);
		}
		if (!StringUtils.isBlank(content)) {
			where.append(" AND content LIKE ? ");
			paramList.add(content);
		}
		return new MapperResult(sqlCountRows + where, paramList);
	}

	/**
	 * 处理了tenant_id作为条件时为空字符串的问题
	 */
	public MapperResult updateConfigInfoAtomicCas(MapperContext context) {
		List<Object> paramList = new ArrayList<>();

		paramList.add(context.getUpdateParameter(FieldConstant.CONTENT));
		paramList.add(context.getUpdateParameter(FieldConstant.MD5));
		paramList.add(context.getUpdateParameter(FieldConstant.SRC_IP));
		paramList.add(context.getUpdateParameter(FieldConstant.SRC_USER));
		paramList.add(context.getUpdateParameter(FieldConstant.APP_NAME));
		paramList.add(context.getUpdateParameter(FieldConstant.C_DESC));
		paramList.add(context.getUpdateParameter(FieldConstant.C_USE));
		paramList.add(context.getUpdateParameter(FieldConstant.EFFECT));
		paramList.add(context.getUpdateParameter(FieldConstant.TYPE));
		paramList.add(context.getUpdateParameter(FieldConstant.C_SCHEMA));
		paramList.add(context.getUpdateParameter(FieldConstant.ENCRYPTED_DATA_KEY));
		paramList.add(context.getWhereParameter(FieldConstant.DATA_ID));
		paramList.add(context.getWhereParameter(FieldConstant.GROUP_ID));
		String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
		String tenantIdQuery = "= ?";
		if (StringUtils.isEmpty(tenantId)) {
			tenantIdQuery = "is null";
		} else {
			paramList.add(tenantId);
		}
		paramList.add(context.getWhereParameter(FieldConstant.MD5));
		String sql = "UPDATE config_info SET " + "content=?, md5=?, src_ip=?, src_user=?, gmt_modified="
				+ getFunction("NOW()")
				+ ", app_name=?, c_desc=?, c_use=?, effect=?, type=?, c_schema=?, encrypted_data_key=? "
				+ "WHERE data_id=? AND group_id=? AND tenant_id " + tenantIdQuery + " AND (md5=? OR md5 IS NULL OR md5='')";
		return new MapperResult(sql, paramList);
	}
}
