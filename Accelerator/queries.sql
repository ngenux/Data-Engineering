--Postgres Queries
--Insert records
SELECT relname, n_tup_ins
FROM pg_stat_user_tables
WHERE relname = 'client_dim'; 

--Update records
SELECT relname, n_tup_upd
FROM pg_stat_user_tables
WHERE relname = 'client_dim';

--hits for a table 
SELECT distinct SUT.schemaname , SUT.relname as Used_Table_name,SUT.seq_scan as no_of_times_Accessed,       SUT.last_autoanalyze, tab.table_name, SAI.indexrelname ,       case when SUT.seq_scan >0 then 'Used' else 'Not Used' end as Tables_Usability,       case when SAI.indexrelname  is null then 'Index not used' else 'Index used' end as Index_Usability
FROM Information_schema.tables Tab 
     left join pg_stat_user_tables SUT  on tab.table_name = SUT.relname
     left join pg_stat_all_indexes SAI on tab.table_name = SAI.relname 
and SUT.schemaname = SAI.schemaname
