# spark-jobs


### Manually

- Create jar
```bash
# At the root of the repo
sbt clean assembly
```
The jar file will be at the folder : ./target/scala-2.13/app.jar

---

- Create docker image
```bash
docker build -t ddcj/spark-job:omea-pocv1 .
```


### CI/CD


```bash
git push ddcj/spark-job:omea-pocv1
```