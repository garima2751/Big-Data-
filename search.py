# necessary libraries in the code
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import regexp_replace,trim,col,lower,split,size,count,countDistinct,log10,explode,round,sum,desc
from pyspark.ml.feature import Tokenizer

# function to remove the punctuations by converting all entries into lower
def remove_punctuations(col):
    return trim(lower(regexp_replace(col,'[^\sa-zA-Z0-9]',''))).alias('text_entry')


# function to create datafarme with id and text_entry after removing punctuation    
def sentence_data(df_data):
    df_data2 = df_data.select(df_data._id , remove_punctuations(df_data.text_entry))
    only_words = Tokenizer(inputCol = 'text_entry' , outputCol="tokens")
    df_data3 = only_words.transform(df_data2)
    return df_data3

# function to create dataframe with tf-idf scores    
def indexing(df1):
	# removing the punctuations in text_entry
	df2 = df1.select(df1._id, remove_punc(df1.text_entry))
	# tokenizing the text_entry
	tokens = Tokenizer(inputCol = 'text_entry' , outputCol="words") 
	# transforming the tokenized output into data frames 
	df2 = tokens.transform(df1)  
	# exploding the dataframe on the basis of tokens                                         
	df4_exp = df2.select(df2._id,df2.text_entry,explode(df2.words).alias('tokens'))
	# performing groupBy on id and token and counting the tokens as term frequency
	tf_data = df4_exp.groupBy("_id","tokens").agg(count("tokens").alias("tf")) 
	# performing groupBy on tokens and count the distinct id as document frequency
	df_data = df4_exp.groupBy("tokens").agg(countDistinct("_id").alias("df")) 
	# calculating the inverse document frequency diving total no of documents to the document frequency  
	idf_data = df_data.withColumn('idf' , (111396.0)/df_data['df'])
	# taking the log base 10 of idf calculated in the previous step
	idf_data = idf_data.withColumn("IDF" , log10("idf")) 
	# joining the column with tf-idf 
    tokensWithTfIdf= tf_data.join(idf_data,"tokens","left").withColumn("tf_idf",col("tf")*col("IDF")) 
	return tokensWithTfIdf

# function to search queries with the value of N
def search_words(query,N,tokensWithTfIdf,df):  
	# the query comes we will split it into tokens using space as a delimiter                                                                                                                                                         
    query_split = query.split(' ')     
    # n defines the length of the query                                                                                                                                               
    n = len(query_split)      
    # filter rows on the basis of query tokens and tokensWithTfIdf database and filter those rows which has tokens similar in query                                                                                                                                                        
    filter_data = tokensWithTfIdf.filter(col("tokens").isin([r for r in query_split]))    
    # performing groupBy operation on id and applying aggregating function on tf-idf values                                                                                            
    tf_idf_sum = filter_data.groupBy("_id").agg(sum("tf_idf").alias("total"))  
    # performing groupBy operation on id and counting the intersection of words appeared in each document with the query                                                                                                       
    occurances_data = filter_data.groupBy("_id").count().toDF('_id','occurances')  
    # joining the two dataframes obtained  tf_idf_sum and occurances_data                                                                                                   
    search_data = tf_idf_sum.join(occurances_data,"_id","inner") 
    # calculating the scores by multiplying sum of tf_idf to the occurances/n obtained in occurances_data step                                                                                                                  
    search_data = search_data.withColumn("score",col("occurances")/n * col("total")) 
    # callinf sentence data function
    df2 = sentence_data(df) 
    # joining dataframe with score to the data frame having id and text_entry                                                                                                
    search_data = search_data.join(df2,"_id","inner") 
    # rounding score to 3 decimal places                                                                                                                                
    search_data = search_data.select(search_data._id , round(search_data.score,3).alias("score") , search_data.text_entry)  
    # sorting the scores in descending order                                                           
    search_data = search_data.sort(desc("score"))
    # collecting the dataframe in variable                                                                                                                                     
    search_data = search_data.limit(N).rdd.map(lambda x: (x._id, x.score,str(x.text_entry))).collect()                                                                                 
    # for loop for printing tuple obtained in collecting dataframe
    for tuple in search_data:                                                                                                                                                         
        print(tuple)       

# main function which will create the object of SQLContext()
def main(sc):
    context = SQLContext(sc)
    # loading the data from local machine
    df = context.read.json("/user/root/garima/finalterm/shakespeare_full.json")
    # calling indexing function
    tfidfdata = indexing(df)
    # searching for query
    search_words("to be or not", 1 ,tfidfdata , df)
    #search_words("to be or not", 3 , tfidfdata ,df)
    #search_words("to be or not", 5 , tfidfdata ,df)
    #search_words("so far so ", 1 , tfidfdata ,df)
    #search_words("so far so ", 3 , tfidfdata ,df)
    #search_words("so far so", 5 , tfidfdata ,df)
    #search_words("if you said so", 1 , tfidfdata ,df)
    #search_words("if you said so", 3 , tfidfdata ,df)
    #search_words("if you said so", 5 , tfidfdata ,df)


# processing of python file starts here    
if __name__  == "__main__":
	# setting spark context
    conf = SparkConf().setAppName("MyApp")
    sc = SparkContext(conf = conf)
    # calling main function
    main(sc)
    sc.stop()

#spark-submit --master yarn-client --executor-memory 512m --num-executors 3 --executor-cores 1 --driver-memory 512m tfidfserach.py