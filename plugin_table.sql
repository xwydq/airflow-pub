-- auto-generated definition
create table dagx_conf2py
(
    id        bigint unsigned auto_increment,
    uuid      varchar(128)                        null,
    status    tinyint   default 0                 null comment '0：失败；1：成功',
    scan_time timestamp default CURRENT_TIMESTAMP null,
    `desc`    text                                null comment '原因描述',
    constraint dagx_conf2py_id_uindex
        unique (id)
);

create index idx__st
    on dagx_conf2py (scan_time);

create index idx__uuid
    on dagx_conf2py (uuid);

alter table dagx_conf2py
    add primary key (id);


-- auto-generated definition
create table dagx_dag
(
    id          bigint unsigned auto_increment
        primary key,
    uuid        varchar(128)                          null,
    name        varchar(128)                          null,
    owner       varchar(64) default 'admin'           null,
    action      varchar(64) default ''                null,
    version     int         default 0                 null,
    crond       varchar(64) default ''                null,
    start_date  datetime                              null,
    end_date    datetime                              null,
    conf        mediumtext                            null,
    is_delete   tinyint(1)  default 0                 null,
    create_time datetime    default CURRENT_TIMESTAMP null,
    update_time datetime    default CURRENT_TIMESTAMP null on update CURRENT_TIMESTAMP,
    constraint idx__name
        unique (name),
    constraint idx__uuid
        unique (uuid)
)
    charset = utf8mb4;

create index idx__owner
    on dagx_dag (owner);


-- auto-generated definition
create table dcmp_dag
(
    id                    int auto_increment
        primary key,
    dag_name              varchar(100) not null,
    version               int          not null,
    category              varchar(50)  not null,
    editing               tinyint(1)   not null,
    editing_user_id       int          null,
    editing_user_name     varchar(100) null,
    last_editor_user_id   int          null,
    last_editor_user_name varchar(100) null,
    updated_at            datetime(6)  not null,
    editing_start         datetime(6)  null,
    last_edited_at        datetime(6)  null,
    approved_version      int          not null,
    approver_user_id      int          null,
    approver_user_name    varchar(100) null,
    last_approved_at      datetime(6)  null,
    constraint dag_name
        unique (dag_name)
)
    charset = utf8mb4;

create index approved_version
    on dcmp_dag (approved_version);

create index category
    on dcmp_dag (category);

create index editing
    on dcmp_dag (editing);

create index editing_start
    on dcmp_dag (editing_start);

create index last_approved_at
    on dcmp_dag (last_approved_at);

create index last_edited_at
    on dcmp_dag (last_edited_at);

create index updated_at
    on dcmp_dag (updated_at);



-- auto-generated definition
create table dcmp_dag_conf
(
    id                 int auto_increment
        primary key,
    dag_id             int          not null,
    dag_name           varchar(100) not null,
    action             varchar(50)  not null,
    version            int          not null,
    conf               mediumtext   not null,
    creator_user_id    int          null,
    creator_user_name  varchar(100) null,
    created_at         datetime(6)  not null,
    approver_user_id   int          null,
    approver_user_name varchar(100) null,
    approved_at        datetime(6)  null
)
    charset = utf8mb4;

create index action
    on dcmp_dag_conf (action);

create index approved_at
    on dcmp_dag_conf (approved_at);

create index created_at
    on dcmp_dag_conf (created_at);

create index dag_id
    on dcmp_dag_conf (dag_id);

create index dag_name
    on dcmp_dag_conf (dag_name);

create index version
    on dcmp_dag_conf (version);



-- auto-generated definition
create table dcmp_user_profile
(
    id                           int auto_increment
        primary key,
    user_id                      int         not null,
    is_superuser                 tinyint(1)  not null,
    is_data_profiler             tinyint(1)  not null,
    is_approver                  tinyint(1)  not null,
    updated_at                   datetime(6) not null,
    created_at                   datetime(6) not null,
    approval_notification_emails text        not null
)
    charset = utf8mb4;

create index created_at
    on dcmp_user_profile (created_at);

create index is_approver
    on dcmp_user_profile (is_approver);

create index is_data_profiler
    on dcmp_user_profile (is_data_profiler);

create index is_superuser
    on dcmp_user_profile (is_superuser);

create index updated_at
    on dcmp_user_profile (updated_at);

create index user_id
    on dcmp_user_profile (user_id);

