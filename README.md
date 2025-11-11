# db_clean_tool
A universal db data cleaning tool
# features
- Delete historical data based on time
- Automatically search for foreign keys(combining auto-discovered FKs and manual business relations), cycle detection with edge-path tracking to avoid infinite recursion in reverse/looped relations
- Depth-first cascade deletion across related tables to honor foreign key constraints.
- Supports skipping specified tables and columns
- Supports backing up data to CSV
- Supports DryRun mode

Currently, postgresql is supported. For other DBS, please verify by yourself

# db_clean_tool
一个通用的 db 清理工具
# features
- 基于时间进行历史数据删除
- 自动发现外键(支持与手动配置合并)，自动避免恶性循环检测
- 对相关表进行深度优先级联删除，以满足外键约束。
- 支持跳过指定表与列
- 支持备份数据到 CSV
- 支持 DryRun 模式

目前支持 postgresql， 其他 db 请自行验证
