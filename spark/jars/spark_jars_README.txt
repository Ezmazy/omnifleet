============================================================
OmniFleet V003 - Spark JARs: how to download them
============================================================

WHY THESE JARS EXIST
--------------------
Spark does not ship with the connectors we need. We add 7 extra JAR files so
Spark can talk to:
  - MinIO / S3        (hadoop-aws + aws-java-sdk-bundle)
  - Kafka             (spark-sql-kafka + spark-token-provider-kafka +
                       kafka-clients + commons-pool2)
  - PostgreSQL        (postgresql JDBC driver)

WHERE THEY GO
-------------
Put all 7 JARs in THIS folder:   omnifleet_v003\spark\jars\
docker-compose mounts this folder into the spark container at:
                                 /usr/local/spark/jars/extra/

THE 7 FILES YOU NEED
--------------------
  hadoop-aws-3.3.4.jar
  aws-java-sdk-bundle-1.12.262.jar
  postgresql-42.7.3.jar
  spark-sql-kafka-0-10_2.12-3.5.3.jar
  spark-token-provider-kafka-0-10_2.12-3.5.3.jar
  kafka-clients-3.4.1.jar
  commons-pool2-2.11.1.jar

IMPORTANT: the versions must match exactly.
  - Spark connector version (3.5.3) must match your Spark version.
  - Scala version in the name is 2.12 (the _2.12 part) - must match Spark's Scala.
  - hadoop-aws (3.3.4) must match the Hadoop version Spark was built with.
Mixing versions is the #1 cause of "ClassNotFound" / "NoSuchMethod" errors.


============================================================
OPTION A - Windows PowerShell (run from D:\omnifleet_v003)
============================================================
Downloads all 7 JARs straight into spark\jars\ .

# make sure the folder exists
New-Item -ItemType Directory -Force -Path .\spark\jars | Out-Null
cd .\spark\jars

# 1. hadoop-aws  (lets Spark read/write s3a:// = MinIO)
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" -OutFile "hadoop-aws-3.3.4.jar"

# 2. aws-java-sdk-bundle  (the AWS SDK hadoop-aws depends on)
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" -OutFile "aws-java-sdk-bundle-1.12.262.jar"

# 3. postgresql JDBC driver  (lets Spark write to Postgres staging)
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar" -OutFile "postgresql-42.7.3.jar"

# 4. spark-sql-kafka  (the Kafka source/sink for Structured Streaming)
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar" -OutFile "spark-sql-kafka-0-10_2.12-3.5.3.jar"

# 5. spark-token-provider-kafka  (auth helper the kafka connector needs)
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar" -OutFile "spark-token-provider-kafka-0-10_2.12-3.5.3.jar"

# 6. kafka-clients  (the underlying Kafka client library)
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" -OutFile "kafka-clients-3.4.1.jar"

# 7. commons-pool2  (connection pooling the kafka connector needs)
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" -OutFile "commons-pool2-2.11.1.jar"

# back to project root
cd ..\..

# check you got all 7
dir .\spark\jars


============================================================
OPTION B - Linux / Mac / WSL (run from the project root)
============================================================

mkdir -p spark/jars
cd spark/jars

curl -L -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
curl -L -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
curl -L -O https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar
curl -L -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar
curl -L -O https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar
curl -L -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar
curl -L -O https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

cd ../..
ls -lh spark/jars


============================================================
VERIFY (after downloading, either OS)
============================================================
You should have exactly 7 .jar files. Quick size sanity check:
  - aws-java-sdk-bundle is the big one  (~280 MB) - if it is only a few KB,
    the download failed (you saved an error page). Delete and retry.
  - the others range from ~100 KB to ~30 MB.

If a file is tiny or won't load in Spark, it usually means a partial/failed
download - delete that one file and re-run only its command.


============================================================
DOWNLOAD URLS (reference - all from Maven Central, repo1.maven.org)
============================================================
hadoop-aws-3.3.4.jar
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
aws-java-sdk-bundle-1.12.262.jar
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
postgresql-42.7.3.jar
  https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar
spark-sql-kafka-0-10_2.12-3.5.3.jar
  https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar
spark-token-provider-kafka-0-10_2.12-3.5.3.jar
  https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar
kafka-clients-3.4.1.jar
  https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar
commons-pool2-2.11.1.jar
  https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar


============================================================
WHAT EACH JAR IS FOR (one line each)
============================================================
hadoop-aws ................. Spark <-> S3/MinIO filesystem (s3a://)
aws-java-sdk-bundle ........ AWS SDK that hadoop-aws calls under the hood
postgresql ................. JDBC driver so Spark can write Postgres staging
spark-sql-kafka-0-10 ....... Kafka source/sink for Structured Streaming
spark-token-provider-kafka . delegation-token auth helper for the Kafka connector
kafka-clients .............. core Kafka client library
commons-pool2 .............. object pooling the Kafka connector requires
