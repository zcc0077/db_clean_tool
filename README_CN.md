# db_clean_tool
一个通用的 db 清理工具
> 运行:
```bash
pip install -r requirements.txt
python -m db_cleaner.cli
```
## Features
- 基于时间进行历史数据删除
- 自动发现外键(支持与手动配置合并)，自动避免恶性循环检测
- 对相关表进行深度优先级联删除，以满足外键约束。
- 支持跳过指定表与列
- 支持备份数据到 CSV
- 支持 DryRun 模式

目前支持 postgresql， 其他 db 请自行验证

## Workflow
1. 构建关系图：
   1. 读取手动关系（related）并规范化为 schema.table 与列映射。
   2. 自动发现当前主表的外键子关系（若开启），与手动关系合并去重。
   3. 应用跳过策略（skip_tables/skip_columns）。
2. 选择一批主表键：
   1.按 date_column < cutoff_date 选出最多 batch_size 条父表键（支持复合键）。
3. 干跑（dry_run=true）路径：
   1. 对每个子关系：
      1.生成用于匹配的父列值（若父列不在当前键则先 SELECT 补齐）。
      2. 统计子表将删除的行数（COUNT … IN (VALUES …)）。
      3.递归到子表，重复上述步骤。
   2. 最后统计当前父表将删除的行数（SELECT COUNT(*) … IN (VALUES …)）。
   3. 打印各表“将删总计”汇总（可选），不做任何修改。
4. 实际删除（dry_run=false）路径：
   1. 可选：SET LOCAL statement_timeout，降低超时。
   2. 递归删除（深度优先）：
      1. 对每个子关系，先 SELECT 子表主键集（不在父层直接删子），并递归进入子层。
      2. 子层完成后，在当前层删除父表（DELETE … IN (VALUES …)）。
   3. 提交事务（commit）；任何异常立即回滚（rollback）。
   4. 归档（可选）：递归过程中为每张将删表先 SELECT 全行缓存在内存，提交成功后统一写 CSV。
## Configuration
```yaml
db_uri: "postgresql://{user}:{pwd}@{db_ip}:5432/{db_table}"
log_file: "./cleaner_prod.log"
log_console: false
log_rotate:
  type: "size"
  max_bytes: 10485760
  backup_count: 10

dry_run: false
skip_tables: ["tenant"]
skip_columns: []

tables:
  - name: "alert"
    enable: true
    auto_discover_related: true
    key_columns: ["id"]            
    date_column: "timestamp"        
    expire_days: 45   # recommend set a time of 7 or 10 days each time (e.g. 90->80->70->....)
    batch_size: 10000 # recommend start from 500
    time_out: 180
    archive: true
    archive_path: "./archive"
    disable_cutoff: true
    conditions:
      - column: "timestamp"
        op: ">="
        value: "2025-09-29 00:00:00"
      - column: "timestamp"
        op: "<"
        value: "2025-09-30 00:00:00"
      - raw_sql: "event->>'name' LIKE %s"
        params: ["test123%"]    
    related:
      - name: "alertenrichment"
        parent_table: "alert"
        mapping:
          parent_columns: ["id"]
          child_columns: ["alert_fingerprint"]
      - name: "workflowtoalertexecution"
        parent_table: "alert"
        mapping:
          parent_columns: ["id"]
          child_columns: ["event_id"]
      - name: "workflowexecution"
        parent_table: "workflowtoalertexecution"
        mapping:
          parent_columns: ["workflow_execution_id"]
          child_columns: ["id"]
      - name: "workflowexecutionlog"
        parent_table: "workflowexecution"
        mapping:
          parent_columns: ["id"]
          child_columns: ["workflow_execution_id"]
      - name: "lastalert"
        parent_table: "alert"
        mapping:
          parent_columns: ["id"]
          child_columns: ["alert_id"]
      - name: "lastalerttoincident"
        parent_table: "lastalert"
        mapping:
          parent_columns: ["tenant_id", "fingerprint"]
          child_columns: ["tenant_id", "fingerprint"]
  - name: "enrichmentevent"
    enable: true
    auto_discover_related: false
    key_columns: ["id"]            
    date_column: "timestamp"        
    expire_days: 45
    batch_size: 20000
    time_out: 180
    archive: true
    archive_path: "./archive"
```
- 关键字段。
  - db_uri：数据库连接的URI，包括用户、密码、主机、端口和数据库名称。
  - dry_run：如果为true，打印删除次数，但不执行。
  - skip_tables：要跳过的表；支持的模式。表名或短表名。
  - skip_columns：过滤关系时要跳过的列。
    - tables：每个表的清理设置。
    - enable：是否对该表进行清理。
    - auto_discover_related：如果为true，则扫描系统目录以查找级联删除的外键关系。
    - key_columns：当前通用级联实现中需要的；使用表的主键列。
    - date_column：用于确定历史行的时间戳列。
    - expire_days：有多少天的记录才有资格被删除。
    - batch_size：每批处理的父键数。
    - time_out：每批语句超时时间，单位为秒。
    - archive：如果为true，则在删除前将批处理的行归档到CSV。
    - archive_path CSV文件存放路径。
    - disable_cutoff：禁用expire_days，主要用于条件中的时间范围。
    - conditions（可选）：表查询条件，详见[conditions]（#conditions）。
    - related（可选）：手动分配相关表
### Conditions
```yaml
tables:
  - name: "alert"
    conditions:
      - column: "column_name"
        op: "operator"
        value: "value"
      - raw_sql: "raw SQL expression"
        params: ["parameter_list"]
```
### Supported Operators
#### 1. Comparison Operators
```yaml
conditions:
  # Equals
  - column: "tenant_id"
    op: "="
    value: "tenant-123"
  
  # Not equals
  - column: "status"
    op: "!="
    value: "deleted"
  
  # Greater than/Less than
  - column: "timestamp"
    op: ">="
    value: "2024-01-01T00:00:00Z"
  
  # Less than or equal
  - column: "retry_count"
    op: "<="
    value: 5
```
#### 2. LIKE Pattern Matching
```yaml
conditions:
  # Prefix matching
  - column: "provider_id"
    op: "LIKE"
    value: "prometheus%"
  
  # Suffix matching
  - column: "fingerprint"
    op: "LIKE"
    value: "%_error"
  
  # Contains matching
  - column: "alert_hash"
    op: "LIKE"
    value: "%abc123%"
  
  # NOT LIKE
  - column: "provider_type"
    op: "NOT LIKE"
    value: "test_%"
```
#### 3. IN / NOT IN List Matching
```yaml
conditions:
  # IN - matches any value in the list
  - column: "provider_type"
    op: "IN" 
    value: ["prometheus", "grafana", "datadog"]
  
  # NOT IN - excludes values in the list
  - column: "status"
    op: "NOT IN"
    value: ["active", "pending"]
```
#### 4. NULL Checks
```yaml
conditions:
  # IS NULL
  - column: "provider_id"
    op: "IS NULL"
    # value not required
  
  # IS NOT NULL
  - column: "alert_hash"
    op: "IS NOT NULL"
```
### JSON Field Queries (using raw_sql)

#### 1. Basic JSON Field Extraction
```yaml
conditions:
  # Exact match for JSON string field
  - raw_sql: "event->>'status' = %s"
    params: ["resolved"]

  # Pattern matching for JSON field
  - raw_sql: "event->>'name' LIKE %s"
    params: ["%wxdipo%"]
  
  # Numeric comparison
  - raw_sql: "(event->>'priority')::int > %s"
    params: [3]
```
#### 2. Nested JSON Fields
```yaml
conditions:
  # Access nested field event.payload.emshost
  - raw_sql: "event->'payload'->>'emshost' = %s"
    params: ["den01wx060ccm01"]
  
  # Pattern matching on nested field
  - raw_sql: "event->'payload'->>'emshost' LIKE %s"
    params: ["den01%"]
  
  # Multi-level nesting event.details.metadata.region
  - raw_sql: "event->'details'->'metadata'->>'region' = %s"
    params: ["us-west"]
```
#### 3. JSON Array Queries
```yaml
conditions:
  # Array containment check (requires jsonb conversion)
  - raw_sql: "event->'fingerprint_fields'::jsonb @> %s::jsonb"
    params: ['"name"']
  
  # Array length check
  - raw_sql: "json_array_length(event->'fingerprint_fields') > %s"
    params: [2]
  
  # Array element existence check
  - raw_sql: "EXISTS (SELECT 1 FROM json_array_elements_text(event->'fingerprint_fields') AS f WHERE f = %s)"
    params: ["emshost"]
```
#### 4. JSON Time Fields
```yaml
conditions:
  # Time field comparison in JSON
  - raw_sql: "(event->>'resolvedTime')::timestamp >= %s"
    params: ["2024-01-01T00:00:00Z"]
  
  # Time range query
  - raw_sql: "(event->>'resolvedTime')::timestamp BETWEEN %s AND %s"
    params: ["2024-01-01T00:00:00Z", "2024-12-31T23:59:59Z"]
```
### Important Notes

#### 1. Parameter Safety
- Always use `%s` placeholders, never concatenate strings directly
- Parameters in the `params` array are automatically escaped to prevent SQL injection
#### 2. JSON Type Conversion
```yaml
# ❌ Incorrect: Direct comparison may have type mismatch
- raw_sql: "event->>'priority' > 3"

# ✅ Correct: Explicit type conversion
- raw_sql: "(event->>'priority')::int > %s"
  params: [3]
```
