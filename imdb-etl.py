from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

#define data source and output
#S3_DATA_SOURCE_PATH = ('s3://imdb-project-bucket/bronze/')
#S3_DATA_OUTPUT_PATH = ('s3://imdb-transformed-data/')

#S3 Credentials
AWS_ACCESS_KEY_ID = ('')
AWS_SECRET_ACCESS_KEY = ('')

#create a spark session
#print("Creating a Spark session ...")
spark = SparkSession.builder.appName('DAMG7370 PROJECT')\
        .config('spark.hadoop.fs.s3a.access.key', AWS_ACCESS_KEY_ID)\
        .config('spark.hadoop.fs.s3a.secret.key', AWS_SECRET_ACCESS_KEY)\
        .getOrCreate()

title_basics = spark.read.csv('s3://imdb-raw-data/bronze/title.basics.tsv', sep='\t', header=True, inferSchema=True)
name_basics = spark.read.csv(
    "s3://imdb-raw-data/bronze/name.basics.tsv",
    sep="\t",
    header=True,
    nullValue="\\N",
    inferSchema=True
)

title_ratings = spark.read.csv('s3://imdb-raw-data/bronze/title.ratings.tsv', sep='\t', header=True, inferSchema=True)
title_principals = spark.read.csv('s3://imdb-raw-data/bronze/title.principals.tsv', sep='\t', header=True, inferSchema=True)
title_akas = spark.read.csv('s3://imdb-raw-data/bronze/title.akas.tsv', sep='\t', header=True, inferSchema=True)
title_episodes = spark.read.csv('s3://imdb-raw-data/bronze/title.episode.tsv', sep='\t', header=True, inferSchema=True)

def remove_duplicates(df):
    return df.drop_duplicates()

def replace_nulls(df):
    return df.replace('\\N','nan')

def convert_to_numeric(df, column_names):
    for column in column_names:
        df = df.withColumn(column, df[column].cast("double"))
    return df

def convert_to_boolean(df, column_names):
    for column in column_names:
        df = df.withColumn(column, df[column].cast("boolean"))
    return df

# clean and transform the name-basics data
name_basics = replace_nulls(name_basics)
name_basics = name_basics.dropna()
name_basics = remove_duplicates(name_basics)
name_basics = convert_to_numeric(name_basics, ['birthYear', 'deathYear'])
name_basics = name_basics[name_basics['birthYear'] >= 1000]
name_basics = name_basics.drop("primaryProfession", "knownForTitles")
name_basics = name_basics.withColumnRenamed("nconst","nameID")


#clean and transform the title-basics data
title_basics = replace_nulls(title_basics)
title_basics = remove_duplicates(title_basics)
title_basics = title_basics.dropna()
title_basics = title_basics.drop("genres")
title_basics  = convert_to_numeric(title_basics , ['startYear', 'endYear', 'runtimeMinutes'])
title_basics  = convert_to_boolean(title_basics , ['isAdult'])
title_basics  = title_basics.withColumnRenamed("tconst","TitleID")

#clean and transform the title-ratings data
title_ratings = replace_nulls(title_ratings)
title_ratings = title_ratings.dropna()
title_ratings= convert_to_numeric(title_ratings, ['averageRating', 'numVotes'])
title_ratings = remove_duplicates(title_ratings)
title_ratings = title_ratings.withColumnRenamed("tconst","TitleID").withColumnRenamed("numVotes","TotalVotes")

#clean and transform the principals data
title_principals = replace_nulls(title_principals)
title_principals = title_principals.dropna()
title_principals = title_principals.drop("job", "characters")
title_principals = title_principals.withColumnRenamed("tconst","TitleID").withColumnRenamed("nconst","nameid")


#clean and transform the akas data
title_akas = replace_nulls(title_akas)
title_akas = title_akas.dropna()
title_akas = remove_duplicates(title_akas)
title_akas= convert_to_boolean(title_akas, ['isOriginalTitle'])
title_akas = title_akas.drop("language", "attributes", "types", "ordering")

#clean and transform the title-repisodes data
title_episodes = replace_nulls(title_episodes)
title_episodes = convert_to_numeric(title_episodes, ['seasonNumber', 'episodeNumber'])
title_episodes = remove_duplicates(title_episodes)
title_episodes  = title_episodes.withColumnRenamed("tconst","TitleID")

#merge title baisics and title ratings data
titleRating = title_basics.join(title_ratings, on='TitleID', how='inner')

#merege title-basics and akas data
titleAkas = title_basics.join(title_akas, on='TitleID', how='inner')

#Save transformed data to S3 Bucket
#print("Saving Process Data to S3 Bucket ....")
name_basics.write.mode('overwrite').csv('s3://imdb-cleaned-data/name_basics.tsv', sep="\t", header=True)
title_episodes.write.mode('overwrite').csv('s3://imdb-cleaned-data/episodes.tsv', sep="\t", header=True)
title_principals.write.mode('overwrite').csv('s3://imdb-cleaned-data/principals.tsv', sep="\t", header=True)
titleRating.write.mode('overwrite').csv('s3://imdb-cleaned-data/mereged_titleRating.tsv', sep="\t", header=True)
titleAkas.write.mode('overwrite').csv('s3://imdb-cleaned-data/merged_titleAkas.tsv', sep="\t", header=True)
#title_basics.write.mode('overwrite').csv('s3://imdb-cleaned-data/title_basics.csv', header=True)
#title_akas.write.mode('overwrite').csv('s3://imdb-cleaned-data/akas.csv', header=True)
#title_ratings.write.mode('overwrite').csv('s3://imdb-cleaned-data/ratings.csv', header=True)





print("Done! Process Completed")