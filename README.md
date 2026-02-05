# spark-jobs


### Manually

- Create jar
```bash
# At the root of the repo
export SBT_OPTS="-Xms2G -Xmx4G -XX:+UseG1GC"
sbt clean assembly
```
The jar file will be at the folder : ./target/scala-2.13/app.jar

---


### WITH DOCKER

- Create docker image
```bash
docker build -t ddcj/spark-job:omea-pocv1 .
# The jar will be build in the docker image
```


### SUBMIT WITH SPARK-SUBMIT

```bash
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=password
```

```bash
./spark-submit \
  --class CreateIcebergTable \
  /home/jeth/Projects/spark-dev-repo/target/scala-2.13/app.jar \
  s3a://datalake/schemas/ \
  voice # sms # data


type=voice;./spark-submit \
  --class PopulateBronzeTables \
  /home/jeth/Projects/spark-dev-repo/target/scala-2.13/app.jar \
  s3a://datalake/$type/ \
  $type \
  bronze.$type


./spark-submit \
  --class VoiceSilverTable \
  /home/jeth/Projects/spark-dev-repo/target/scala-2.13/app.jar \
  "2026-02-05" \
  bronze.voice \
  silver.voice

./spark-submit \
  --class VoiceGoldTables \
  /home/jeth/Projects/spark-dev-repo/target/scala-2.13/app.jar \
  "2026-02-05" \
  silver.voice


./spark-submit \
  --class SmsSilverTable \
  /home/jeth/Projects/spark-dev-repo/target/scala-2.13/app.jar \
  "2026-02-05" \
  bronze.sms \
  silver.sms

./spark-submit \
  --class SmsGoldTables \
  /home/jeth/Projects/spark-dev-repo/target/scala-2.13/app.jar \
  "2026-02-05" \
  silver.sms


./spark-submit \
  --class DataSilverTable \
  /home/jeth/Projects/spark-dev-repo/target/scala-2.13/app.jar \
  "2026-02-05" \
  bronze.data \
  silver.data

./spark-submit \
  --class DataGoldTables \
  /home/jeth/Projects/spark-dev-repo/target/scala-2.13/app.jar \
  "2026-02-05" \
  silver.data


```