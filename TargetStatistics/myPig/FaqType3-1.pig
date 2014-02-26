/* Author: lei.zhang@xiaoi.com (Zhang Lei)
* 统计每日各种answer type的类型，缺省统计全部类型 由stat_alldata.py调用
*--------------------------------------------------------------------------------------
*/
register /work/xiaoi/pig/xiaoiUDFs.jar
--register /usr/local/pig/lib/hbase-0.94.14.jar
--register /usr/local/pig/lib/zookeeper-3.4.5.jar
--register /usr/local/pig/lib/guava-11.0.2.jar
--register /usr/local/pig/lib/avro-1.5.3.jar
--register /usr/local/pig/lib/json-simple-1.1.jar
--register /usr/local/pig/lib/piggybank.jar
--register /usr/local/hbase/lib/protobuf-java-2.4.0a.jar

%default hdfs_input_dir '/pig/data/'
%default hdfs_output_dir '/experiment/shmcc/output/countAnsType/'
%default siml 0.0
%default simr 1.0
%default date 2000-01-01

data = load '$hdfs_input_dir' using PigStorage('|') as 
(
	visit_time:chararray,
	session_id:chararray,
	user_id:chararray,
	question:chararray,
	answer:chararray,
	answer_type:int,
	faq_id:chararray,
	faq_name:chararray,
	keyword:chararray,
	city:chararray,
	brand:chararray,
	similarity:double,
	module_id:chararray
);

A = foreach data generate faq_id,faq_name,answer_type,similarity;
filterAns = filter A by similarity >= $siml and similarity <= $simr;

grpData	= group filterAns by faq_name;
genData	= foreach grpData generate FLATTEN(group) as faqname, COUNT(filterData) as count;
odrData	= order genData by $1 desc;

Result = foreach foreachAll generate xiaoiUDFs.GenerateRowKey('stat', 'faqAnstype', '$date'), count, faqname;
--dump ansTypeResult;
--store ansTypeResult into '$hdfs_output_dir';
store Result into 'shmcc_stat' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('stat_cf:count stat_cf:faqname');

/* like this:
hbase(main):014:0> scan 'shmcc_stat'
            ROW                          COLUMN+CELL
            stat_anstype_2012-10-01_0    column=stat_cf:count, timestamp=1388476126541, value=1
            stat_anstype_2012-10-01_0    column=stat_cf:percent, timestamp=1388476126541, value=0.5     
            stat_anstype_2012-10-01_101  column=stat_cf:count, timestamp=1388476126542, value=1
            stat_anstype_2012-10-01_101  column=stat_cf:percent, timestamp=1388476126542, value=0.5
            stat_anstype_2012-10-02_0    column=stat_cf:count, timestamp=1388476259058, value=1
*/
