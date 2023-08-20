# StarRocks-Kettle-Connector-plugin Readme

# Why

目前，StarRocks兼容并支持DataX、Flink以及Spark这三种高效的数据处理框架，从而实现数据的有效写入到StarRocks中。然而，需要注意的是，这三种框架的操作均基于命令行或代码，这可能对非技术人员构成一定的挑战。因此，为了增加易用性并优化用户体验，我们正在扩展StarRocks的Kettle Connector，以实现直观的、可视化的数据导入操作，使得无论技术背景如何，用户都能够方便快捷地进行数据导入。

Kettle是一个流行的ETL工具，它提供了一种可视化的图形界面，用户可以通过拖拽组件、配置参数等方式来构建数据处理流程。这种直观的操作方式大大简化了数据处理和导入的过程，使得用户可以更加便捷地处理数据。此外，Kettle还提供了丰富的操作组件库，用户可以根据自己的需求选择合适的组件，实现各种复杂的数据处理任务。

通过扩展StarRocks对Kettle的连接功能，用户不仅可以实现更方便的数据导入，还可以利用Kettle的操作组件库，提供更便捷、更灵活的数据处理和导入方式。用户可以更加方便地从各种数据源读取数据，然后通过Kettle的数据处理流程，将处理后的数据导入到StarRocks。

# What

StarRocks Kettle Connector实现了Kettle的一个插件，它用于在StarRocks和Kettle之间建立连接，以实现众多数据源数据向StarRocks导入和ETL（Extract, Transform, Load）功能。通过此插件，可以将Kettle的强大数据处理和转换功能与StarRocks的高性能数据存储和分析能力相结合，从而实现更加灵活和高效的数据处理流程。

使用StarRocks Kettle Connector的场景包括：

1. 数据集成：当需要从不同的数据源中抽取数据，进行数据清洗和转换，最后将数据加载到StarRocks中进行分析和查询时，可以使用此功能来实现数据集成和ETL。
2. 复杂数据处理：当数据处理流程比较复杂，需要多个数据转换步骤和数据源连接时，可以利用Kettle的可视化界面来设计和配置ETL工作流程，最后将数据记载到StarRocks，提高开发效率和灵活性。
3. 数据转换和整合：当需要对原始数据进行复杂的转换和整合，以满足特定的数据分析和查询需求时，可以使用Kettle的强大数据转换功能来实现。

通过StarRocks Kettle Connector，用户可以获得以下好处：

1. 便捷性：利用Kettle的可视化界面，可以以图形化方式设计和配置复杂的ETL工作流程，简化了从不同数据源向StarRocks的数据加载过程，降低学习成本。
2. 灵活性：通过与Kettle的连接，扩展了StarRocks的数据处理能力，使得用户可以根据自己的需求选择适合的工具和方式来进行数据处理。
3. 高性能：StarRocks作为一个高性能的数据存储和分析引擎，与Kettle的连接可以将高效的数据加载与复杂的数据转换和整合相结合，从而提高数据处理的性能。

# How

## Kettle安装

如果已经装有Ｋｅｔｔｌｅ可跳过此步骤。

首先，需要从Github上下载对应版本的[Kettle源码](https://github.com/pentaho/pentaho-kettle)，并根据Readme构建Kettle项目。Kettle项目使用的是Maven框架，需要如下准备：

- Maven,version3+
- Java JDK 11
- 使用该[settings.xml](https://raw.githubusercontent.com/pentaho/maven-parent-poms/master/maven-support-files/settings.xml) 在/.m2 文件夹

最后生成Kettle应用压缩包：<源码目录>\assemblies\pdi-ce\target\pdi-ce-x.x.x.x-xxx.zip

![img](image/2.jpg)

## Kettle启动

解压上一步得到的pdi-ce-x.x.x.x-xxx.zip压缩包，得到Kettle应用data-integration无需安装。data-integration文件包含如下启动文件：

![img](image/3.jpg)

通过Spoon.bat(Windows)或Spoon.sh(Linux)启动Kettle图形界面，如下显示证明启动成功。

![img](image/1.jpg)

## 导入StarRocks Kettle Connector 插件

- 下载StarRocks Kettle Connector的插件源码，将其进行打包编译得到**assemblies/plugin/target/StarRocks-Kettle-Connector-plugin-x.x.x.x-xxx.zip**包。
- 将**StarRocks-Kettle-Connector-plugin-x.x.x.x-xxx.zip**包放在**data-integration\plugins**文件目录下。
- 将插件包直接解压到当前文件夹，生成**StarRocks-Kettle-Connector**文件包。
- 启动Spoon，**转换/批量**加载中即可看到**StarRocks Kettle Connector**插件。

![img](image/6.jpg)

拖拽或者双击将StarRocks Kettle Connector插件生成新的step，之后再双击右侧新添加的StarRocks Kettle Connector插件step即可配置数据导入参数。

![](image/9.jpg)

## 参数说明

| 参数                                            | 是否必填 | 默认值                     | 数据类型 | 描述                                                                                                                                                                                                                          |
| :---------------------------------------------- | -------- | -------------------------- | -------- |-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 步骤名称-Step name                              | 是       | StarRocks Kettle Connector | String   | 该步骤名称                                                                                                                                                                                                                       |
| Http Url                                        | 是       | 无                         | String   | FE 的 HTTP Server 连接地址。格式为 `<fe_host1>:<fe_http_port1>;<fe_host2>:<fe_http_port2>`，可以提供多个地址，使用英文分号 (;) 分隔。例如 `192.168.xxx.xxx:8030;192.168.xxx.xxx:8030`。                                                                    |
| JDBC Url                                        | 是       | 无                         | String   | FE 的 MySQL Server 连接地址。格式为 `jdbc:mysql://<fe_host>:<fe_query_port>`。                                                                                                                                                        |
| 数据库-DataBase Name                            | 是       | 无                         | String   | StarRocks 目标数据库的名称。                                                                                                                                                                                                         |
| 目标表-Table Name                               | 是       | 无                         | String   | StarRocks 目标数据表的名称。                                                                                                                                                                                                         |
| 用户名-User                                     | 是       | 无                         | String   | 用于访问 StarRocks 集群的用户名。该账号需具备 StarRocks 目标数据表的写权限。有关用户权限的说明，请参见[用户权限](https://docs.starrocks.io/zh-cn/latest/administration/User_privilege)。                                                                                 |
| 密码-Password                                   | 否       | 无                         | String   | 用于访问 StarRocks 集群的用户密码。若没有密码则不用填写。                                                                                                                                                                                          |
| 格式-Format                                     | 是       | CSV                        | String   | Stream Load 导入时的数据格式。取值为 `CSV` 或者 `JSON`。                                                                                                                                                                                   |
| 列分割符-Column Sepatator                       | 否       | \t                         | String   | 用于指定源数据文件中的列分隔符。如果不指定该参数，则默认为 `\t`，即 Tab。必须确保这里指定的列分隔符与源数据文件中的列分隔符一致。该参数当选择CSV格式的时候必须填写。<br />**说明**<br />StarRocks 支持设置长度最大不超过 50 个字节的 UTF-8 编码字符串作为列分隔符，包括常见的逗号 (,)、Tab 和 Pipe (）                                         |
| Json Paths                                      | 否       | 无                         | String   | 用于指定待导入的字段的名称。仅在使用匹配模式导入 JSON 数据时需要指定该参数。当格式选择JSON时填写该参数。参见[导入 JSON 数据时配置列映射关系](https://docs.starrocks.io/zh-cn/latest/sql-reference/sql-statements/data-manipulation/STREAM LOAD#导入-json-数据时配置列映射关系).                   |
| 一次导入最大字节-Max Bytes                      | 否       | 94371840(90M)              | String   | 数据攒批的大小，达到该阈值后将数据通过 Stream Load 批量写入 StarRocks。取值范围：[64MB, 10GB]。                                                                                                                                                           |
| 刷新频率-Scanning Frequency                     | 否       | 50                         | String   | 数据刷新的时间，每隔多长时间进行一次数据刷新写入。参数单位为毫秒，取值大于等于50毫秒。                                                                                                                                                                                |
| 导入作业最大容错率-Max Filter Ratio             | 否       | 0                          | String   | 用于指定导入作业的最大容错率，即导入作业能够容忍的因数据质量不合格而过滤掉的数据行所占的最大比例。取值范围：0~1。默认值：0。更多说明，请参见 [STREAM LOAD](https://docs.starrocks.io/zh-cn/latest/sql-reference/sql-statements/data-manipulation/STREAM LOAD#opt_properties)。                   |
| StarRocks连接超时时间-Connect Timeout           | 否       | 1000                       | String   | 连接 `load-url` 的超时时间。取值范围：[100, 60000]。                                                                                                                                                                                      |
| Stream Load载入数据超时时间-Stream Load Timeout | 否       | 600                        | String   | Stream Load 超时时间，单位为秒。                                                                                                                                                                                                      |
| 部分导入-Partial Update                         | 否       | 否                         |          | StarRocks v2.2 起，主键模型表支持部分更新，可以选择只更新部分指定的列。若勾选实现部分导入需要在“部分导入行”中填写要导入的列名。                                                                                                                                                    |
| 部分导入行-Partial Update Columns               | 否       | 无                         | String   | 需要部分更新的列名。需要填写所要写入的目标表中对应的列名。各列命之间要以英文逗号隔开“,”，例如：col1,col2col3                                                                                                                                                              |
| 是否支持更新和删除-Enable Upsert Delete         | 否       | 无                         |          | StarRocks 目前支持 UPSERT 和 DELETE 操作，不支持一次作业区分UPSERT和DELETE，只能对一次导入单独实现UPSERT和DELETE。<br />  **UPSERT**: 该操作用于插入或更新数据。如果数据已存在（基于主键/唯一键），它将更新该数据；如果数据不存在，它将插入新数据。<br /> **DELETE**: 该操作用于删除符合条件的数据记录。需要指定删除的条件，满足该条件的所有记录都将被删除。 |
| Upsert or Delete                                | 否       | 无                         | String   | 当勾选“是否支持更新和删除”时需要选择是执行UPSERT或DELETE操作。若未选择则不执行更新或删除操作。                                                                                                                                                                      |
| 表字段-Table field                              | 否       | 无                         | String   | StarRocks目标表中各列的名称。需要与流字段一一对应。                                                                                                                                                                                              |
| 流字段-Stream field                             | 否       | 无                         | String   | 上一步骤传输过来的数据列名称。从上一步骤传递的数据列名称和类型必须与StarRocks目标表的数据格式和大小完全匹配。                                                                                                                                                                 |

根据插件ui提示输入StarRocks连接配置信息。

```Java
'http-url'='fe_ip1:8030,fe_ip2:8030,fe_ip3:8030'
'jdbc-url'='jdbc:mysql://fe_ip:9030'
'username'='root'
'password'=''
'database-name'='kettle_test'
'table-name'='kettle_test'
// The format of the data to be loaded. The value can be CSV or JSON. 
// The default is CSV. 
'format'='CSV'
// Starting from version 2.4, partial column updates in the primary key model are supported. You can specify the columns to be updated through the following two attributes, 
// and you need to explicitly add the '__op' column at the end of 'partial-update-columns','k1,k2,k3'.
'partial-update'='true'
'partial-update-columns'='k1,k2,k3'
// The maximum size of data that can be loaded into StarRocks at a time. 
// Valid values: 64 MB to 10 GB. 
'maxbytes'=94371840
// Specifies the maximum fault tolerance rate for the import job, which is the maximum 
// proportion of data rows that the import job can tolerate filtered out due to substandard data quality. 
// Value range: 0~1. Default value: 0.
'max_filter_ratio'=0
// Timeout period for connecting to the load-url. 
// Valid values: 100 to 60000
'connect-timeout'=1000
// Stream Load timeout period, in seconds.
'timeout'=600
```

## StarRocks-Kettle数据类型对应关系

### Kettle数据类型

1. **String**：存储字符串或文本信息。
2. **Date**：存储日期信息。日期被存储为从1970-01-01 00:00:00.000 GMT开始的毫秒数。因此，可以保存任何日期和时间，从公元前至公元后。日期类型的默认掩码为yyyy/MM/dd HH:mm:ss.SSS。
3. **Boolean**：存储逻辑值，即True/False。
4. **Integer**：存储整数值。所有整数都被当作长整型(Long)处理，范围在-9223372036854775808到9223372036854775807之间。
5. **Number**：用于存储浮点数。这是一种双精度浮点类型，具有至少15位的精度。
6. **BigNumber**：用于存储任意精度的数字，适合用于精确的科学计算。
7. **Binary**：用于存储二进制对象。
8. **Timestamp**：这是一个扩展的日期类型，允许更好地在数据库中处理日期和时间的组合。
9. **Internet Address**：存储Internet地址，主要是为了验证这些地址的正确性。

### StarRocks数据类型以及与Kettle对应

| Kettle           | StarRocks                                                                                                  |
| ---------------- |------------------------------------------------------------------------------------------------------------|
| String           | CHAR、STRING、VARCHAR、JSON                                                                                   |
| Date             | DATE、DATETIME                                                                                              |
| Boolean          | BOOLEAN                                                                                                    |
| Integer          | TINYINT 、SMALLINT 、INT 、BIGINT                                                                             |
| Number           | DOUBLE、FLOAT                                                                                               |
| BigNumber        | LARGEINT、[DECIMAL](https://docs.starrocks.io/zh-cn/latest/sql-reference/sql-statements/data-types/DECIMAL) |
| Binary           | 暂不支持                                                                                                       |
| Timestamp        | DATETIME、DATE                                                                                              |
| Internet Address | STRING                                                                                                     |
| serializable     | 暂不支持                                                                                                       |



## 使用示例

本节介绍如何使用StarRocks-Kettle Connector插件从本地文件系统导入CSV或JSON格式的数据。

### 导入CSV格式的数据

#### 准备工作

1. 在本地文件系统中创建一个 CSV 格式的数据文件 `example1.csv`。文件一共包含三列，分别代表用户 ID、用户姓名和用户得分，如下所示：

```Plain
1,Lily,23
2,Rose,23
3,Alice,24
4,Julia,25
```

2. 在数据库 `kettle_test` 中创建一张名为 `student` 的主键模型表。表包含 `id`、`name` 和 `score` 三列，主键为 `id` 列，如下所示：

~~~mysql
CREATE TABLE `student`
(
    `id` int(11) NOT NULL COMMENT "用户 ID",
    `name` varchar(65533) NULL COMMENT "用户姓名",
    `score` int(11) NOT NULL COMMENT "用户得分"
)
ENGINE=OLAP
PRIMARY KEY(`id`)
DISTRIBUTED BY HASH(`id`);
~~~



#### 使用Kettle读取csv文件数据

1. 填写文件信息，并将列分割符改成“,”与CSV文件中一样。当CSV文件中没有列头行时取消列头行的勾选。

![img](image/5.jpg)

2. 获取字段信息

> 点击**获取字段**，得到CSV中的字段信息和类型。
>
> 若CSV文件中没有包含头行信息，则**名称**列将会显示**Field_xxx**，为了后续步骤区分字段名称可以自行命名。
>
> 字段类型Kettle会自动识别，如果需要更改可直接下拉菜单更改类型。

![](image/10.jpg)

3. 数据预览

当配置完信息后，可点击最后的**预览**按钮预览需要导入的数据。

![](image/11.jpg)

#### 向StarRocks中导入数据

1. 配置StarRocks数据导入参数。(最终开发会实现如上ui输入框的格式来进行参数的输入)

```Java
'scan-url'='127.0.0.1:8030'
'jdbc-url'='jdbc:mysql://127.0.0.1:9030'
'username'='root'
'password'=''
'database-name'='kettle_test'
'table-name'='test_table'
'format'='CSV'
'semantic'='exactly-once'
'loadProps'={}
```

1. 点击开始按钮执行导入作业

![img](image/7.jpg)

1. 查询导入结果

```SQL
MySQL [kettle_test]> select * from test_table;
+------+-----------+-------+
| id   | name      | scores|
+------+-----------+-------+
|    1 | Lily      |   23  |
|    2 | Rose      |   23  |
|    3 | Alice     |   24  |
|    4 | Julia     |   25  |
+------+-----------+-------+
4 rows in set (0.02 sec)
```

# Limitation

- 不支持at-least-once和exactly-once导入方式。
- 只支持CSV和JSON两种数据格式。

## 目前还未实现
- 没有添加addHeaders(getSinkStreamLoadProperties())。未获取多余的StarRocks的参数配置。
- 如果想要实现表格中的一部分数据导入StarRocks中的一部分，中间要加上一个过滤步骤。
- 对于数据的更新插入和删除功能还没有实现分别的删除和更新插入，只能单独实现。todo
- 在kettle中现实的FieldTable名称应该和数据库的名称一样。
- 可以尝试根据dialog中的stepname命名.labelPrefix()，
- **在ui中可以实现自动搜索目标数据库中的表。（还未实现）**
- 映射需要实现目标表和源表的字段顺序正确，如果对应不对则需更改StarRocks目标表的字段顺序
- 目前只支持StarRocks的版本为2.4以上，只实现了v2。


- 获取不到ErrorUrl得不到Errorlog。
- kttle版本依赖
- JDBC加载多个地址
- Starrocks的时间类型数据是如何传输的