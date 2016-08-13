source /home/recsys/.bashrc;
#=======================================================#
#===================预处理方法参数======================#
#=======================================================#
# -i：输入
# -o：输出
# -m：计算方法
# -k：每个商品推荐出的商品个数
# -t：临时文件夹
# -p：是否使用前处理
pre=TRUE

while getopts "i:o:m:k:p" arg #选项后面的冒号表示该选项需要参数
do
    case $arg in
        i)
            IN_TABLE=$OPTARG
            echo "input file is $IN_TABLE" #参数存在$OPTARG中
            ;;
        o)
            OUT_TABLE=$OPTARG
            echo "the output file is $OUT_TABLE"
            ;;
        m)
            method=$OPTARG
            echo "the the similarity method is $method"
            ;;
        k)
            K=$OPTARG
            echo "the top k items recommended for main item is $K"
            ;;
        t)
            tmp=$OPTARG
            echo "the temp file is $tmp"
            ;;
        p)
            #当给定这个参数时不计算前处理
            pre=FALSE 
        ?)  #当有不认识的选项的时候arg为?
            echo "unkonw argument"
    exit 1
;;
esac
done



# method=manh

#input=/user/recsys/app.db/cart_last_day_id
# IN_TABLE=""

# output=/user/recsys/dev.db/zhongchao/cf/manh
# tmp=/user/recsys/dev.db/zhongchao/cf/tmp
# N=100
#=======================================================#
#===================数据预处理方法======================#
#=======================================================#
if [ ${pre} = 'TRUE' ]
then
echo "Preprocess the data......"
# hive -e "
# drop table ${IN_TABLE}_gt2;
# create table ${IN_TABLE}_gt2
# as
# select a.* from
# ${IN_TABLE} a
# join
# (select b.uuid1d from
# (select uuid1d,count(1) n from ${IN_TABLE} group by uuid1d) b where b.n>1) c on (a.uuid1d=c.uuid1d)
# "
#计算uuid1d对应的id
s="
set mapreduce.job.reduce.slowstart.completedmaps=0.95;
drop table ${IN_TABLE}_pin_id;
create table ${IN_TABLE}_pin_id
as
select cc.uuid1d,cc.id-1 id from
(select bb.uuid1d,row_number(bb.num) as id from 
(select distinct 1 as num, uuid1d from ${IN_TABLE} order by num) bb) cc
order by id
"
hive -e "$s"
[ $? -ne 0 ] && { echo "$s EXIT";exit 2;}
#计算sku对应的id
s="
set mapreduce.job.reduce.slowstart.completedmaps=0.95;
drop table ${IN_TABLE}_sku_id;
create table ${IN_TABLE}_sku_id
as
select cc.sku,cc.id-1 id from
(select bb.sku,row_number(bb.num) as id from 
(select distinct 1 as num, sku from ${IN_TABLE} order by num) bb) cc
order by id
"
hive -e "$s"
[ $? -ne 0 ] && { echo "$s EXIT";exit 2;}
#关联出uuid1d对应的数据
s="
set mapreduce.job.reduce.slowstart.completedmaps=0.95;
drop table ${IN_TABLE}_id_tmp;
create table ${IN_TABLE}_id_tmp
as
select b.id,concat_ws('_',cast(bb.id as string),cast(a.weight as string)) sku from
${IN_TABLE} a
join
${IN_TABLE}_pin_id b on (a.uuid1d=b.uuid1d)
join
${IN_TABLE}_sku_id bb on (a.sku=bb.sku)
"
hive -e "$s"
[ $? -ne 0 ] && { echo "$s EXIT";exit 2;}


s="
set mapreduce.job.reduce.slowstart.completedmaps=0.95;
drop table ${IN_TABLE}_id;
create table ${IN_TABLE}_id
as
select concat_ws('#',collect_set(c.sku)) from ${IN_TABLE}_id_tmp
group by id
"
hive -e "$s"
[ $? -ne 0 ] && { echo "$s EXIT";exit 2;}

fi

N=`hive -e "select max(id)+1 from ${IN_TABLE}_pin_id"`
lib=`echo ${IN_TABLE}_id | awk  'BEGIN{FS="."} {print $1}'`
tab=`echo ${IN_TABLE}_id | awk  'BEGIN{FS="."} {print $2}'`
input=/user/recsys/${lib}.db/${tab}
output=${tmp}/${method}_output

#=======================================================#
#===============itemSimilarity计算方法==================#
#=======================================================#
echo "Calculate the item to item similarity using cf......"

#Cosine
if [ $method = 'cosi' ]
then
    #计算pre_1
    hadoop fs -rm -r ${tmp}/pre_1
    cd /data0/recsys/zhongchao/recsys_work/cf;
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar \
    -Dmapreduce.job.reduce.slowstart.completedmaps=0.9 \
    -Dstream.non.zero.exit.is.failure=false \
    -Dmapred.reduce.tasks=20 \
    -Dmapred.map.child.java.opts=-Xmx2048m \
    -Dmapred.reduce.child.java.opts=-Xmx2048m \
    -input ${input} \
    -output ${tmp}/pre_1 \
    -mapper "python ./cfCosiPreMapper_1.py" \
    -file "cfCosiPreMapper_1.py" \
    -reducer "python ./cfCosiPreReducer_1.py" \
    -file "cfCosiPreReducer_1.py"

    norm_file='norm_'`echo $RANDOM`
    norm_sort_file='norm_sort'`echo $RANDOM`
    # echo $tmp_file1
    echo $norm_file
    echo $norm_sort_file
    hadoop fs -getmerge ${tmp}/pre_1 ${norm_file}
    cat ${norm_file} | sort -k1 -n > ${norm_sort_file}


    #计算pre_2
    hadoop fs -rm -r ${tmp}/pre_2
    cd /data0/recsys/zhongchao/recsys_work/cf;
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar \
    -Dmapreduce.job.reduce.slowstart.completedmaps=0.9 \
    -Dstream.non.zero.exit.is.failure=false \
    -Dmapred.reduce.tasks=0 \
    -Dmapred.map.child.java.opts=-Xmx2048m \
    -Dmapred.reduce.child.java.opts=-Xmx2048m \
    -input ${input} \
    -output ${tmp}/pre_2 \
    -mapper "python ./cfCosiPreMapper_2.py ${norm_sort_file}" \
    -file "cfCosiPreMapper_2.py" \
    -file ${norm_sort_file}

    #cat hdfs_data.txt | python ./cfCosiPreMapper_2.py  
    #计算相似度
    hadoop fs -rm -r ${output};
    cd /data0/recsys/zhongchao/recsys_work/cf;
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar \
    -Dmapreduce.job.reduce.slowstart.completedmaps=0.9 \
    -Dstream.non.zero.exit.is.failure=false \
    -Dmapred.reduce.tasks=80 \
    -Dmapred.map.child.java.opts=-Xmx2048m \
    -Dmapred.reduce.child.java.opts=-Xmx2048m \
    -input ${tmp}/pre_2 \
    -output ${output} \
    -mapper "python ./cfCosiSimMapper.py" \
    -file "cfCosiSimMapper.py" \
    -reducer "python ./cfCosiSimReducer.py" \
    -file "cfCosiSimReducer.py"


    \rm -r ${norm_file}
    \rm -r ${norm_sort_file}
    #cat pre_2 | python ./cfCosiSimMapper.py | sort | python ./cfCosiSimReducer.py
fi

#Pearson correlation
if [ $method = 'pear' ]
then
    #对数据进行归一化
    #计算pre_1

    hadoop fs -rm -r ${tmp}/pre1
    cd /data0/recsys/zhongchao/recsys_work/cf;
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar \
    -Dmapreduce.job.reduce.slowstart.completedmaps=0.9 \
    -Dstream.non.zero.exit.is.failure=false \
    -Dmapred.reduce.tasks=20 \
    -Dmapred.map.child.java.opts=-Xmx2048m \
    -Dmapred.reduce.child.java.opts=-Xmx2048m \
    -input ${input} \
    -output ${tmp}/pre1 \
    -mapper "python ./cfPearPreMapper_1.py" \
    -file "cfPearPreMapper_1.py" \
    -reducer "python ./cfPearPreReducer_1.py" \
    -file "cfPearPreReducer_1.py"

    norm_file='norm_'`echo $RANDOM`
    norm_sort_file='norm_sort'`echo $RANDOM`
    # echo $tmp_file1
    echo $norm_file
    echo $norm_sort_file
    hadoop fs -getmerge ${tmp}/pre1 ${norm_file}
    cat ${norm_file} | sort -k1 -n > ${norm_sort_file}


    #计算相似度
    hadoop fs -rm -r ${output};
    cd /data0/recsys/zhongchao/recsys_work/cf;
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar \
    -Dmapreduce.job.reduce.slowstart.completedmaps=0.9 \
    -Dstream.non.zero.exit.is.failure=false \
    -Dmapred.reduce.tasks=100 \
    -Dmapred.map.child.java.opts=-Xmx2048m \
    -Dmapred.reduce.child.java.opts=-Xmx2048m \
    -input ${input} \
    -output ${output} \
    -mapper "python ./cfPearSimMapper.py" \
    -file "cfPearSimMapper.py" \
    -reducer "python ./cfPearSimReducer.py ${norm_sort_file} $N" \
    -file "cfPearSimReducer.py" \
    -file ${norm_sort_file}

    \rm -r ${norm_file}
    \rm -r ${norm_sort_file}
    #cat hdfs_data.txt | python ./cfPearSimMapper.py | sort | python ./cfPearSimReducer.py norm_sort23579 100
fi



#Euclidean distance
if [ $method = 'eurl' ]
then
    #计算norm
    hadoop fs -rm -r ${tmp}/pre1
    cd /data0/recsys/zhongchao/recsys_work/cf;
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar \
    -Dmapreduce.job.reduce.slowstart.completedmaps=0.9 \
    -Dstream.non.zero.exit.is.failure=false \
    -Dmapred.reduce.tasks=20 \
    -Dmapred.map.child.java.opts=-Xmx2048m \
    -Dmapred.reduce.child.java.opts=-Xmx2048m \
    -input ${input} \
    -output ${tmp}/pre1 \
    -mapper "python ./cfNormMapper.py" \
    -file "cfNormMapper.py" \
    -reducer "python ./cfNormReducer.py ${method}" \
    -file "cfNormReducer.py"

    #cat hdfs_data.txt | python ./cfNormMapper.py | sort -k1 -n | python ./cfNormReducer.py > norm_sort.txt  
    #计算相似度
    norm_file='norm_'`echo $RANDOM`
    norm_sort_file='norm_sort'`echo $RANDOM`
    # echo $tmp_file1
    echo $norm_file
    echo $norm_sort_file
    hadoop fs -getmerge ${tmp}/pre1 ${norm_file}
    cat ${norm_file} | sort -k1 -n > ${norm_sort_file}

    hadoop fs -rm -r /user/recsys/dev.db/zhongchao/cf/euro;
    cd /data0/recsys/zhongchao/recsys_work/cf;
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar \
    -Dmapreduce.job.reduce.slowstart.completedmaps=0.9 \
    -Dstream.non.zero.exit.is.failure=false \
    -Dmapred.reduce.tasks=80 \
    -Dmapred.map.child.java.opts=-Xmx2048m \
    -Dmapred.reduce.child.java.opts=-Xmx2048m \
    -input ${input} \
    -output ${output} \
    -mapper "python ./cfSimMapper.py" \
    -file "cfSimMapper.py" \
    -reducer "python ./cfSimReducer.py ${method} $N ${norm_sort_file}" \
    -file "cfSimReducer.py" \
    -file ${norm_sort_file}

    \rm -r ${norm_file}
    \rm -r ${norm_sort_file}
    #cat pre | python ./cfSimMapper.py | sort -k1 -n | head | python ./cfSimReducer.py logl 100 | sort -k12 -n > text.txt   
fi


#Log-likelihood ratio
if [ $method = 'logl' ]
then
    #预处理
    hadoop fs -rm -r ${tmp}/pre1
    cd /data0/recsys/zhongchao/recsys_work/cf;
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar \
    -Dmapreduce.job.reduce.slowstart.completedmaps=0.9 \
    -Dstream.non.zero.exit.is.failure=false \
    -Dmapred.reduce.tasks=0 \
    -Dmapred.map.child.java.opts=-Xmx2048m \
    -Dmapred.reduce.child.java.opts=-Xmx2048m \
    -input ${input} \
    -output ${tmp}/pre1 \
    -mapper "python ./cfPreMapper.py" \
    -file "cfPreMapper.py"
    #计算norm
    hadoop fs -rm -r ${tmp}/pre2
    cd /data0/recsys/zhongchao/recsys_work/cf;
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar \
    -Dmapreduce.job.reduce.slowstart.completedmaps=0.9 \
    -Dstream.non.zero.exit.is.failure=false \
    -Dmapred.reduce.tasks=20 \
    -Dmapred.map.child.java.opts=-Xmx2048m \
    -Dmapred.reduce.child.java.opts=-Xmx2048m \
    -input ${tmp}/pre1 \
    -output ${tmp}/pre2 \
    -mapper "python ./cfNormMapper.py" \
    -file "cfNormMapper.py" \
    -reducer "python ./cfNormReducer.py ${method}" \
    -file "cfNormReducer.py"


    norm_file='norm_'`echo $RANDOM`
    norm_sort_file='norm_sort'`echo $RANDOM`
    # echo $tmp_file1
    echo $norm_file
    echo $norm_sort_file
    hadoop fs -getmerge ${tmp}/pre2 ${norm_file}
    cat ${norm_file} | sort -k1 -n > ${norm_sort_file}


    #cat hdfs_data.txt | python ./cfNormMapper.py | sort -k1 -n | python ./cfNormReducer.py > norm_sort.txt  
    #计算相似度


    hadoop fs -rm -r ${output};
    cd /data0/recsys/zhongchao/recsys_work/cf;
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar \
    -Dmapreduce.job.reduce.slowstart.completedmaps=0.9 \
    -Dstream.non.zero.exit.is.failure=false \
    -Dmapred.reduce.tasks=80 \
    -Dmapred.map.child.java.opts=-Xmx2048m \
    -Dmapred.reduce.child.java.opts=-Xmx2048m \
    -input ${tmp}/pre1 \
    -output ${output} \
    -mapper "python ./cfSimMapper.py" \
    -file "cfSimMapper.py" \
    -reducer "python ./cfSimReducer.py ${method} $N ${norm_sort_file}" \
    -file "cfSimReducer.py" \
    -file ${norm_sort_file}
    
    [ $? -ne 0 ] && { echo "cfSimReducer 错误";exit 2;}
    
    \rm -r ${norm_file}
    \rm -r ${norm_sort_file}
    #cat pre1 | python ./cfSimMapper.py | sort -k1 -n | python ./cfSimReducer.py logl 12834737 norm_sort28075 | sort -k12 -n > logl.txt   
fi

#Jaccard coeffcient
if [ $method = 'jacc' ]
then
    #预处理
    hadoop fs -rm -r ${tmp}/pre1
    cd /data0/recsys/zhongchao/recsys_work/cf;
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar \
    -Dmapreduce.job.reduce.slowstart.completedmaps=0.9 \
    -Dstream.non.zero.exit.is.failure=false \
    -Dmapred.reduce.tasks=0 \
    -Dmapred.map.child.java.opts=-Xmx2048m \
    -Dmapred.reduce.child.java.opts=-Xmx2048m \
    -input ${input} \
    -output ${tmp}/pre1 \
    -mapper "python ./cfPreMapper.py" \
    -file "cfPreMapper.py"
    #计算norm
    hadoop fs -rm -r ${tmp}/pre2
    cd /data0/recsys/zhongchao/recsys_work/cf;
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar \
    -Dset mapreduce.job.reduce.slowstart.completedmaps=0.9 \
    -Dstream.non.zero.exit.is.failure=false \
    -Dmapred.reduce.tasks=20 \
    -Dmapred.map.child.java.opts=-Xmx2048m \
    -Dmapred.reduce.child.java.opts=-Xmx2048m \
    -input  ${tmp}/pre1\
    -output ${tmp}/pre2 \
    -mapper "python ./cfNormMapper.py" \
    -file "cfNormMapper.py" \
    -reducer "python ./cfNormReducer.py ${method}" \
    -file "cfNormReducer.py"

    #cat hdfs_data.txt | python ./cfNormMapper.py | sort -k1 -n | python ./cfNormReducer.py > norm_sort.txt  
    #计算相似度
    norm_file='norm_'`echo $RANDOM`
    norm_sort_file='norm_sort'`echo $RANDOM`
    # echo $tmp_file1
    echo $norm_file
    echo $norm_sort_file
    hadoop fs -getmerge ${tmp}/pre2 ${norm_file}
    cat ${norm_file} | sort -k1 -n > ${norm_sort_file}

    hadoop fs -rm -r ${output};
    cd /data0/recsys/zhongchao/recsys_work/cf;
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar \
    -Dmapreduce.job.reduce.slowstart.completedmaps=0.9 \
    -Dstream.non.zero.exit.is.failure=false \
    -Dmapred.reduce.tasks=80 \
    -Dmapred.map.child.java.opts=-Xmx2048m \
    -Dmapred.reduce.child.java.opts=-Xmx2048m \
    -input ${tmp}/pre1 \
    -output ${output} \
    -mapper "python ./cfSimMapper.py" \
    -file "cfSimMapper.py" \
    -reducer "python ./cfSimReducer.py ${method} $N ${norm_sort_file}" \
    -file "cfSimReducer.py" \
    -file ${norm_sort_file}

    \rm -r ${norm_file}
    \rm -r ${norm_sort_file}
    #cat pre1 | python ./cfSimMapper.py | sort -k1 -n | python ./cfSimReducer.py jacc 100 norm_sort3284 | sort -k12 -n > text.txt   
fi

#Manhattan distance
if [ $method = 'manh' ]
then
    #预处理
    hadoop fs -rm -r ${tmp}/pre1
    cd /data0/recsys/zhongchao/recsys_work/cf;
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar \
    -Dmapreduce.job.reduce.slowstart.completedmaps=0.9 \
    -Dstream.non.zero.exit.is.failure=false \
    -Dmapred.reduce.tasks=0 \
    -Dmapred.map.child.java.opts=-Xmx2048m \
    -Dmapred.reduce.child.java.opts=-Xmx2048m \
    -input ${input} \
    -output ${tmp}/pre1 \
    -mapper "python ./cfPreMapper.py" \
    -file "cfPreMapper.py"
    #计算norm
    hadoop fs -rm -r ${tmp}/pre2
    cd /data0/recsys/zhongchao/recsys_work/cf;
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar \
    -Dmapreduce.job.reduce.slowstart.completedmaps=0.9 \
    -Dstream.non.zero.exit.is.failure=false \
    -Dmapred.reduce.tasks=20 \
    -Dmapred.map.child.java.opts=-Xmx2048m \
    -Dmapred.reduce.child.java.opts=-Xmx2048m \
    -input ${tmp}/pre1 \
    -output ${tmp}/pre2 \
    -mapper "python ./cfNormMapper.py" \
    -file "cfNormMapper.py" \
    -reducer "python ./cfNormReducer.py ${method}" \
    -file "cfNormReducer.py"

    #cat hdfs_data.txt | python ./cfNormMapper.py | sort -k1 -n | python ./cfNormReducer.py > norm_sort.txt  
    #计算相似度
    norm_file='norm_'`echo $RANDOM`
    norm_sort_file='norm_sort'`echo $RANDOM`
    # echo $tmp_file1
    echo $norm_file
    echo $norm_sort_file
    hadoop fs -getmerge ${tmp}/pre2 ${norm_file}
    cat ${norm_file} | sort -k1 -n > ${norm_sort_file}

    hadoop fs -rm -r ${output};
    cd /data0/recsys/zhongchao/recsys_work/cf;
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar \
    -Dmapreduce.job.reduce.slowstart.completedmaps=0.9 \
    -Dstream.non.zero.exit.is.failure=false \
    -Dmapred.reduce.tasks=80 \
    -Dmapred.map.child.java.opts=-Xmx2048m \
    -Dmapred.reduce.child.java.opts=-Xmx2048m \
    -input ${tmp}/pre1 \
    -output ${output} \
    -mapper "python ./cfSimMapper.py" \
    -file "cfSimMapper.py" \
    -reducer "python ./cfSimReducer.py ${method} $N ${norm_sort_file}" \
    -file "cfSimReducer.py" \
    -file ${norm_sort_file}

    \rm -r ${norm_file}
    \rm -r ${norm_sort_file}
    #cat pre | python ./cfSimMapper.py | sort -k1 -n | head | python ./cfSimReducer.py logl 100 | sort -k12 -n > text.txt   
fi
#=======================================================#
#==============后处理，取topK，生成hive表===============#
#=======================================================#
hadoop fs -rm -r ${output}/_SUCCESS
#建立外部临时表
s="
set mapreduce.job.reduce.slowstart.completedmaps=0.9;
drop table ${OUT_TABLE}_t1;
CREATE EXTERNAL TABLE ${OUT_TABLE}_t1(id1 string,id2 string,weight double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${output}'"
hive -e "$s"
[ $? -ne 0 ] && { echo "$s EXIT";exit 2;}
#取临时表中的topK数据
s="
set mapreduce.job.reduce.slowstart.completedmaps=0.9;
drop table ${OUT_TABLE}_t2;
create table ${OUT_TABLE}_t2
as
select id1,id2,weight from
(select id1,id2,weight,row_number(id1) id from
(select id1,id2,weight from ${OUT_TABLE}_t1 distribute by id1 sort by id1,weight desc) a) b
where b.id<${K}
"
#关联出最终结果
hive -e "$s"
[ $? -ne 0 ] && { echo "$s EXIT";exit 2;}

s="
set mapreduce.job.reduce.slowstart.completedmaps=0.9;
drop table ${OUT_TABLE};
create table ${OUT_TABLE}
as
select b.sku sku1,c.sku sku2,a.weight from
${OUT_TABLE}_t2 a
join
${IN_TABLE}_sku_id b on (a.id1=b.id)
join
${IN_TABLE}_sku_id c on (a.id2=c.id)
"
hive -e "$s"
[ $? -ne 0 ] && { echo "$s EXIT";exit 2;}



