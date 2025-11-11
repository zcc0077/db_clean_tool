> Usage:
```bash
pip install -r requirements.txt
python -m db_cleaner.cli
```

# db_clean_tool
A universal db data cleaning tool
# Features
- Delete historical data based on time
- Automatically search for foreign keys(combining auto-discovered FKs and manual business relations), cycle detection with edge-path tracking to avoid infinite recursion in reverse/looped relations
- Depth-first cascade deletion across related tables to honor foreign key constraints.
- Supports skipping specified tables and columns
- Supports backing up data to CSV
- Supports DryRun mode

Currently, postgresql is supported. For other DBS, please verify by yourself
# Workflow
1. Build a relationship diagram
   1. Read the manual relationship (related) and normalize it into a schema.table and column mapping.
   2. Automatically discover the foreign key sub-relationships of the current main table (if enabled) and merge them with manual relationships to remove duplicates.
   3. Apply the skip strategy (skip_tables/skip_columns).
2. Select a batch of primary table keys
   1. Select the maximum number of batch_size parent table keys by date_column < cutoff_date (composite keys are supported).
3. Dry run (dry_run=true) path:
   1. For each sub-relation:
      1. Generate the parent column value for matching (if the parent column is not in the current key, SELECT to complete it first).
      2. COUNT the number of rows to be deleted in the sub-table (COUNT...) IN (VALUES …)
      3. Recurse to the sub-table and repeat the above steps.
   2. Finally, COUNT the number of rows that will be deleted in the current parent table (SELECT COUNT(*)... IN (VALUES …)
   3. Print the "Total to be Deleted" summary of each table (optional), without making any modifications.
4. Actual deletion (dry_run=false) path:
   1. Optional: SET LOCAL statement_timeout to reduce timeout.
   2. Recursive deletion (depth-first)
      1. For each child relationship, first SELECT the primary key set of the child table (do not directly delete the child from the parent layer), and then recursively enter the child layer.
      2. After the child layer is completed, DELETE the parent table at the current layer (DELETE...) IN (VALUES …)
   3. Commit transaction; Any exception will be immediately rolled back.
   4. Archiving (optional) : During the recursive process, SELECT the entire row of each table to be deleted and cache it in memory first. After successful submission, write the CSV uniformly.


# db_clean_tool
一个通用的 db 清理工具
# Features
- 基于时间进行历史数据删除
- 自动发现外键(支持与手动配置合并)，自动避免恶性循环检测
- 对相关表进行深度优先级联删除，以满足外键约束。
- 支持跳过指定表与列
- 支持备份数据到 CSV
- 支持 DryRun 模式

目前支持 postgresql， 其他 db 请自行验证

# Workflow
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
