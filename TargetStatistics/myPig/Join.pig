/*
*通过设置old和new输入目录，可以实现new目录中新出现的问题
*   hdfs_input_dir  Input data path 
*   hdfs_output_dir Output data path
*   siml            Left border of similarity
*   simr            Right border of similarity
*   ansType         Answer_type column in data
*--------------------------------------------------------------------------------------
*/
%default hdfs_input_dir_old '/pig/data2012'
%default hdfs_input_dir_new '/pig/data2013'
%default hdfs_output_dir '/experiment/shmcc/output/dump_newquestion/'
%default siml 0.0
%default simr 1.0
%default ansType 0
SET default_parallel 9;

data_old = load '$hdfs_input_dir_old' using PigStorage('|') as
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

data_new = load '$hdfs_input_dir_new' using PigStorage('|') as
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
                 
);

NEW = foreach data_new generate question;
OLD = foreach data_old generate question;
NEW = DISTINCT NEW;
OLD = DISTINCT OLD;


--filterAns = filter A by similarity >= $siml and similarity <= $simr and answer_type == $ansType;

C = JOIN NEW BY question LEFT OUTER, OLD BY question;
D = FILTER C BY (OLD::question is null);

store D into '$hdfs_output_dir';
--store D into '$hdfs_output_dir' using PigStorage('|');


                                                         
