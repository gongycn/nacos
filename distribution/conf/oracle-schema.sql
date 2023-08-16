create table CONFIG_INFO
(
    ID                 NUMBER(20)         not null
        primary key,
    DATA_ID            VARCHAR2(255 char) not null,
    GROUP_ID           VARCHAR2(128 char),
    CONTENT            CLOB               not null,
    MD5                VARCHAR2(32 char),
    GMT_CREATE         DATE               not null,
    GMT_MODIFIED       DATE               not null,
    SRC_USER           CLOB,
    SRC_IP             VARCHAR2(20 char),
    APP_NAME           VARCHAR2(128 char) default NULL,
    TENANT_ID          VARCHAR2(128 char) default '',
    C_DESC             VARCHAR2(256 char) default NULL,
    C_USE              VARCHAR2(64 char)  default NULL,
    EFFECT             VARCHAR2(64 char)  default NULL,
    TYPE               VARCHAR2(64 char)  default NULL,
    C_SCHEMA           CLOB,
    ENCRYPTED_DATA_KEY CLOB               null
);

create unique index UK_CONFIGINFO_DATAGROUPTENANT
    on CONFIG_INFO (DATA_ID, GROUP_ID, TENANT_ID);

create table CONFIG_INFO_AGGR
(
    ID           NUMBER(20)         not null
        primary key,
    DATA_ID      VARCHAR2(255 char) not null,
    GROUP_ID     VARCHAR2(128 char) not null,
    DATUM_ID     VARCHAR2(255 char) not null,
    CONTENT      CLOB               not null,
    GMT_MODIFIED DATE               not null,
    APP_NAME     VARCHAR2(128 char) default NULL,
    TENANT_ID    VARCHAR2(128 char) default ''
);

create unique index UK_C_DATAGROUPTENANTDATUM
    on CONFIG_INFO_AGGR (DATA_ID, GROUP_ID, TENANT_ID, DATUM_ID);

create table CONFIG_INFO_BETA
(
    ID                 NUMBER(20)         not null
        primary key,
    DATA_ID            VARCHAR2(255 char) not null,
    GROUP_ID           VARCHAR2(128 char) not null,
    APP_NAME           VARCHAR2(128 char)  default NULL,
    CONTENT            CLOB               not null,
    BETA_IPS           VARCHAR2(1024 char) default NULL,
    MD5                VARCHAR2(32 char)   default NULL,
    GMT_CREATE         DATE               not null,
    GMT_MODIFIED       DATE               not null,
    SRC_USER           CLOB,
    SRC_IP             VARCHAR2(20 char)   default NULL,
    TENANT_ID          VARCHAR2(128 char)  default '',
    ENCRYPTED_DATA_KEY CLOB               not null
);

create unique index UK_C_DATAGROUPTENANT
    on CONFIG_INFO_BETA (DATA_ID, GROUP_ID, TENANT_ID);

create table CONFIG_INFO_TAG
(
    ID           NUMBER(20)         not null
        primary key,
    DATA_ID      VARCHAR2(255 char) not null,
    GROUP_ID     VARCHAR2(128 char) not null,
    TENANT_ID    VARCHAR2(128 char) default '',
    TAG_ID       VARCHAR2(128 char) not null,
    APP_NAME     VARCHAR2(128 char) default NULL,
    CONTENT      CLOB               not null,
    MD5          VARCHAR2(32 char)  default NULL,
    GMT_CREATE   DATE               not null,
    GMT_MODIFIED DATE               not null,
    SRC_USER     CLOB,
    SRC_IP       VARCHAR2(20 char)  default NULL
);

create unique index UK_C_DATAGROUPTENANTTAG
    on CONFIG_INFO_TAG (DATA_ID, GROUP_ID, TENANT_ID, TAG_ID);

create table CONFIG_TAGS_RELATION
(
    ID        NUMBER(20)         not null,
    TAG_NAME  VARCHAR2(128 char) not null,
    TAG_TYPE  VARCHAR2(64 char)  default NULL,
    DATA_ID   VARCHAR2(255 char) not null,
    GROUP_ID  VARCHAR2(128 char) not null,
    TENANT_ID VARCHAR2(128 char) default '',
    NID       NUMBER(20)         not null
        primary key
);

create unique index UK_C_CONFIGIDTAG
    on CONFIG_TAGS_RELATION (ID, TAG_NAME, TAG_TYPE);

create index IDX_TENANT_ID
    on CONFIG_TAGS_RELATION (TENANT_ID);

create table GROUP_CAPACITY
(
    ID                NUMBER(20) not null
        primary key,
    GROUP_ID          VARCHAR2(128 char) default '',
    QUOTA             NUMBER(10)         default '0',
    USAGE             NUMBER(10)         default '0',
    MAX_SIZE          NUMBER(10)         default '0',
    MAX_AGGR_COUNT    NUMBER(10)         default '0',
    MAX_AGGR_SIZE     NUMBER(10)         default '0',
    MAX_HISTORY_COUNT NUMBER(10)         default '0',
    GMT_CREATE        DATE       not null,
    GMT_MODIFIED      DATE       not null
);

comment on table GROUP_CAPACITY is '集群、各Group容量信息表';

comment on column GROUP_CAPACITY.ID is '主键ID';

comment on column GROUP_CAPACITY.GROUP_ID is 'Group ID，空字符表示整个集群';

comment on column GROUP_CAPACITY.QUOTA is '配额，0表示使用默认值';

comment on column GROUP_CAPACITY.USAGE is '使用量';

comment on column GROUP_CAPACITY.MAX_SIZE is '单个配置大小上限，单位为字节，0表示使用默认值';

comment on column GROUP_CAPACITY.MAX_AGGR_COUNT is '聚合子配置最大个数，，0表示使用默认值';

comment on column GROUP_CAPACITY.MAX_AGGR_SIZE is '单个聚合数据的子配置大小上限，单位为字节，0表示使用默认值';

comment on column GROUP_CAPACITY.MAX_HISTORY_COUNT is '最大变更历史数量';

comment on column GROUP_CAPACITY.GMT_CREATE is '创建时间';

comment on column GROUP_CAPACITY.GMT_MODIFIED is '修改时间';

create unique index UK_GROUP_ID
    on GROUP_CAPACITY (GROUP_ID);

create table HIS_CONFIG_INFO
(
    ID                 NUMBER             not null,
    NID                NUMBER(20)         not null
        primary key,
    DATA_ID            VARCHAR2(255 char) not null,
    GROUP_ID           VARCHAR2(128 char) not null,
    APP_NAME           VARCHAR2(128 char) default NULL,
    CONTENT            CLOB               not null,
    MD5                VARCHAR2(32 char)  default NULL,
    GMT_CREATE         DATE               not null,
    GMT_MODIFIED       DATE               not null,
    SRC_USER           CLOB,
    SRC_IP             VARCHAR2(20 char)  default NULL,
    OP_TYPE            CHAR(10 char)      default NULL,
    TENANT_ID          VARCHAR2(128 char) default '',
    ENCRYPTED_DATA_KEY CLOB                null
);

create index IDX_GMT_CREATE
    on HIS_CONFIG_INFO (GMT_CREATE);

create index IDX_GMT_MODIFIED
    on HIS_CONFIG_INFO (GMT_MODIFIED);

create index IDX_DID
    on HIS_CONFIG_INFO (DATA_ID);

create table TENANT_CAPACITY
(
    ID                NUMBER(20) not null
        primary key,
    TENANT_ID         VARCHAR2(128) default '',
    QUOTA             NUMBER(10)    default '0',
    USAGE             NUMBER(10)    default '0',
    MAX_SIZE          NUMBER(10)    default '0',
    MAX_AGGR_COUNT    NUMBER(10)    default '0',
    MAX_AGGR_SIZE     NUMBER(10)    default '0',
    MAX_HISTORY_COUNT NUMBER(10)    default '0',
    GMT_CREATE        DATE       not null,
    GMT_MODIFIED      DATE       not null
);

create unique index UK_TENANT_ID
    on TENANT_CAPACITY (TENANT_ID);

create table TENANT_INFO
(
    ID            NUMBER(20)    not null
        primary key,
    KP            VARCHAR2(128) not null,
    TENANT_ID     VARCHAR2(128 char) default '',
    TENANT_NAME   VARCHAR2(128 char) default '',
    TENANT_DESC   VARCHAR2(256 char) default NULL,
    CREATE_SOURCE VARCHAR2(32 char)  default NULL,
    GMT_CREATE    NUMBER(20)    not null,
    GMT_MODIFIED  NUMBER(20)    not null
);

create unique index UK_TENANT_INFO_KPTENANTID
    on TENANT_INFO (KP, TENANT_ID);

create table USERS
(
    USERNAME VARCHAR2(50 char)  not null
        primary key,
    PASSWORD VARCHAR2(500 char) not null,
    ENABLED  CHAR               not null
);

create table ROLES
(
    USERNAME VARCHAR2(50 char) not null,
    ROLE     VARCHAR2(50 char) not null,
    constraint UK_USERNAME_ROLE
        unique (USERNAME, ROLE)
);

create table PERMISSIONS
(
    ROLE       VARCHAR2(50 char)  not null,
    RESOURCES VARCHAR2(512 char) not null,
    ACTION     VARCHAR2(8 char)   not null,
    constraint UK_ROLE_PERMISSION
        unique (ROLE, RESOURCES, ACTION)
);

INSERT INTO users (username, password, enabled) VALUES ('nacos', '$2a$10$EuWPZHzz32dJN7jexM34MOeYirDdFAZm2kuWj7VEOJhhZkDrxfvUu', '1');

INSERT INTO roles (username, role) VALUES ('nacos', 'ROLE_ADMIN');

create sequence SEQ_CONFIG_INFO
    maxvalue 999999999999;

create sequence SEQ_CONFIG_INFO_AGGR
    maxvalue 999999999999;

create sequence SEQ_CONFIG_INFO_BETA
    maxvalue 999999999999;

create sequence SEQ_CONFIG_INFO_TAG
    maxvalue 999999999999;

create sequence SEQ_CONFIG_TAGS_RELATION
    maxvalue 999999999999;

create sequence SEQ_CAPACITY_ENTITY
    maxvalue 999999999999;

create sequence SEQ_HIS_CONFIG_INFO
    maxvalue 999999999999;

create sequence SEQ_TENANT_INFO
    maxvalue 999999999999;


