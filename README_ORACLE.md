# Nacos Oracle 适配问题记录：默认命名空间配置双写与 ORA-00001

> 适用分支：`3.2.4-oracle`（基于 Nacos 3.2.x + 自建 Oracle 数据源插件 `nacos-datasource-plugin-oracle`）
> 问题日期：2026-09-10

## 一、问题描述

在**默认命名空间（public）**下新增配置时，Oracle 的 `config_info` 表中出现了**两条记录**：

| data_id | group_id | tenant_id | 说明 |
|---------|----------|-----------|------|
| xxx     | DEFAULT_GROUP | `null` | 兼容镜像行（Oracle 把 `''` 存成 NULL） |
| xxx     | DEFAULT_GROUP | `public` | 3.x 正式行 |

且之后**修改该配置必定失败**，报错：

```
ERROR got exception. PreparedStatementCallback; ORA-00001: 违反唯一约束条件 (NACOS3.UK_CONFIGINFO_DATAGROUPTENANT)
caused: PreparedStatementCallback; ORA-00001: 违反唯一约束条件 (NACOS3.UK_CONFIGINFO_DATAGROUPTENANT)
;caused: ORA-00001: 违反唯一约束条件 (NACOS3.UK_CONFIGINFO_DATAGROUPTENANT)
```

> 说明：该问题**只影响默认命名空间 public**。自定义命名空间不会触发双写，每条配置只有一行。

## 二、根因分析

这是 **Nacos 3.x 命名空间兼容模式（双写）** 与 **Oracle 空字符串语义** 相互作用的结果。

### 2.1 为什么多出一条 TENANT_ID=null 的行

Nacos 3.x 发布配置时，`ConfigOperationService.publishConfig` 会先把空 namespace 归一化为
`"public"`：

- `config/src/main/java/com/alibaba/nacos/config/server/service/ConfigOperationService.java:90`
  （`NamespaceUtil.processNamespaceParameter`，见
  `common/src/main/java/com/alibaba/nacos/common/utils/NamespaceUtil.java:44`：blank → `"public"`）

随后在正式落库**之前**，执行命名空间兼容迁移双写：

- `ConfigOperationService.java:132` → `ConfigMigrateService.publishConfigMigrate`
  （`config/src/main/java/com/alibaba/nacos/config/server/service/ConfigMigrateService.java:945`）

`publishConfigMigrate` 的逻辑：**当发布目标 namespace 是 `public` 且命名空间兼容模式开启时，
额外写一条 `tenant_id=''` 的镜像行**（为了兼容 2.x 数据库中 public 命名空间的存储形式）。
兼容模式开关 `nacos.config.namespace.compatible.mode` **默认为 true**
（`config/src/main/java/com/alibaba/nacos/config/server/configuration/ConfigCompatibleConfig.java:32,48`，
属性名见 `PropertiesConstant.java:70`）。

于是**一次"新增"实际执行两次 INSERT**：

1. 镜像行：`INSERT ... tenant_id=''`
2. 正式行：`INSERT ... tenant_id='public'`

**关键差异**：MySQL 把 `''` 存为空字符串；**Oracle 把 `''` 一律存为 `NULL`**。
所以镜像行在 Oracle 里就是 `TENANT_ID=NULL` 的那一行。

### 2.2 为什么修改必报 ORA-00001

修改（以及任何第二次发布同一个 dataId+group）会重复执行双写，此时：

1. 双写先执行 `insertOrUpdate(d, g, '')`，其内部先用
   `findConfigInfoState(d, g, '')` 查找已有记录
   （`ExternalConfigInfoPersistServiceImpl.java:1219`），
   SQL 条件是 `tenant_id = ?`（绑定 `''`）。
   **Oracle 中 `tenant_id = NULL` 永远为假** → 查不到那行明明存在的 NULL 镜像 →
   判定"记录不存在" → 转入 **INSERT** 分支。
2. INSERT 的新键为 `(data_id, group_id, NULL)`。Oracle 复合唯一索引的规则是：
   **只有全部键列都为 NULL 的索引项才不入索引**；`data_id`/`group_id` 是 NOT NULL，
   所以 `(d, g, NULL)` 照样入索引、照样强制唯一 → 与第一次新增留下的 NULL 镜像行冲突
   → **ORA-00001**。
3. 该异常从 `insertOrUpdate` 的 `catch(Exception){throw}`
   （`ExternalConfigInfoPersistServiceImpl.java:244`）原样上抛。双写在
   `publishConfig` 中先于正式写入执行（`:132` 早于 `:149-153`），
   **正式行的 UPDATE 根本没轮到执行，整个请求即告失败**。
   日志中的 `got exception. ...` 来自控制台全局异常处理器
   `console/src/main/java/com/alibaba/nacos/console/exception/ConsoleExceptionHandler.java:59`。

### 2.3 完整时序

**新增（第一次，"成功"但产生两行）**

```
publishConfig(ns='' → 'public')
 ├─ publishConfigMigrate(ns='public', 兼容模式=true)
 │   └─ insertOrUpdate(d, g, '')：findConfigInfoState('') 查不到 → INSERT → NULL 镜像行 ✓
 └─ 正式 insertOrUpdate(d, g, 'public')：查不到 → INSERT → 'public' 行 ✓
```

**修改（第二次起，必失败）**

```
publishConfig(ns → 'public')
 ├─ publishConfigMigrate
 │   └─ insertOrUpdate(d, g, '')：findConfigInfoState('') 仍查不到（NULL=NULL 永假）
 │       → INSERT (d,g,NULL) → 撞已有 NULL 行 → ORA-00001 ✗ 请求中断
 └─ 正式 UPDATE（未执行）
```

## 三、解决方案

### 3.1 推荐方案：关闭命名空间兼容模式 + 清理存量数据

全新部署的 Oracle 库没有 2.x 时代的 `tenant_id=''` 旧数据，兼容双写没有任何收益，
直接关闭：

```properties
# application.properties
nacos.config.namespace.compatible.mode=false
```

或以环境变量方式（Spring relaxed binding）：

```
NACOS_CONFIG_NAMESPACE_COMPATIBLE_MODE=false
```

开关关闭后，双写的两条路径都被门禁拦住，不再产生 NULL 行：

- `publishConfigMigrate`：`ConfigMigrateService.java:949`
- `checkChangedConfigMigrateState`：`ConfigMigrateService.java:434`

然后**清理存量脏数据**（先备份）：

```sql
DELETE FROM config_info   WHERE tenant_id IS NULL;
COMMIT;

-- 可选：历史表里同样会有 NULL tenant 的历史行
DELETE FROM his_config_info WHERE tenant_id IS NULL;
COMMIT;
```

### 3.2 验证

1. 重启服务端，确认兼容模式已关闭（观察启动日志或直接验证行为）。
2. 在 public 命名空间新增配置，确认只有一行：

```sql
SELECT id, data_id, group_id, tenant_id, gmt_modified
  FROM config_info
 WHERE data_id = 'your-data-id' AND group_id = 'DEFAULT_GROUP';
-- 期望：仅 1 行，TENANT_ID = 'public'
```

3. 修改该配置并保存，确认不再报 ORA-00001，且行数不变、`gmt_modified` 更新。

### 3.3 如果必须保留兼容模式（不推荐用于 Oracle）

理论上的方言层解法：在 Oracle Mapper 中把所有 `tenant_id = ?`（绑定为 `''` 时）
改写为 `(tenant_id = ? OR tenant_id IS NULL)` 或 `NVL(tenant_id, <哨兵值>) = ...`，
让 NULL 镜像行能被"查到"，从而走 UPDATE 而不是 INSERT。

但存在**根本性矛盾**：Oracle 无法存储空字符串，`''` 与 `'public'`
两种表示形式在 Oracle 里天然无法区分——镜像行"用两种 tenant 值区分新旧两套语义"的
设计前提在 Oracle 上不成立。因此**实用答案就是关闭该开关**。

## 四、相关代码位置速查

| 内容 | 位置 |
|------|------|
| namespace 归一化（blank → public） | `config/.../service/ConfigOperationService.java:90` |
| 正式落库（insertOrUpdate 分支选择） | `config/.../service/ConfigOperationService.java:149` |
| 兼容模式双写入口 | `config/.../service/ConfigMigrateService.java:945` |
| 双写门禁（ns=public 且兼容模式） | `config/.../service/ConfigMigrateService.java:949` |
| 兼容模式开关（默认 true） | `config/.../configuration/ConfigCompatibleConfig.java:32,48` |
| 开关属性名 | `config/.../constant/PropertiesConstant.java:70`（`nacos.config.namespace.compatible.mode`） |
| 变更事件驱动的异步双写 | `config/.../service/ConfigMigrateService.java:431`（`checkChangedConfigMigrateState`） |
| insertOrUpdate（查不到即 INSERT，异常原样上抛） | `config/.../repository/extrnal/ExternalConfigInfoPersistServiceImpl.java:232` |
| findConfigInfoState（`tenant_id = ?` 查询） | `config/.../repository/extrnal/ExternalConfigInfoPersistServiceImpl.java:1219` |
| INSERT 绑定空串（`defaultEmptyIfBlank`） | `config/.../repository/extrnal/ExternalConfigInfoPersistServiceImpl.java:306` |
| UPDATE 的 WHERE 含 `tenant_id = ?` | `config/.../repository/extrnal/ExternalConfigInfoPersistServiceImpl.java:800` |
| 唯一约束定义 | `plugin-default-impl/nacos-default-datasource-plugin/nacos-datasource-plugin-oracle/src/main/resources/META-INF/oracle-schema.sql:40` |

## 五、附注

1. **`oracle-schema.sql` 中的 `DEFAULT ''`**：在 Oracle DDL 里 `''` 字面量等价于 `NULL`，
   因此各表的 `tenant_id VARCHAR2(128) DEFAULT ''` 实际就是 `DEFAULT NULL`，纯装饰性。
   建议显式写成 `DEFAULT NULL` 以免误导。本次问题不在 schema，而在代码层的双写逻辑
   与 Oracle 语义冲突。
2. **NULL 行的其它隐患**：即使不触发 ORA-00001，`tenant_id IS NULL` 的行对所有
   `= / != / LIKE` 条件都是不可见的（例如容量统计 `getTenantIdList` 的
   `tenant_id != 'public'`），会造成统计与清理任务漏数据——这也是必须清理而非
   只关闭开关的原因。
3. **老客户端兼容性**：关闭兼容模式只影响数据库层面的双写；3.x 服务端对请求参数的
   归一化（blank/`public` → `public`）不受影响，客户端行为无感知。
