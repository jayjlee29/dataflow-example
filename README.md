# README
> jdk8 사용

## init

```
gradle init
```


## env
```
export PROJECT_ID=vible-lotus
export BUCKET=vible-couchbase
export DATASET=vible
export TABLE_NAME=vible
export ZONE=us-central1
export REGION=us-central1

```

## service account에 역활 추가후 json 다운로드

### 역활
```
BigQuery 관리자
DataFlow 관리자
DataFlow 작업자
Cloud DataFlow 서비스 에이전트
```

### 환경변수를 이용한 service account 설정
```
export GOOGLE_APPLICATION_CREDENTIALS=/Users/jay/vible/workspace/vible_dataflow/dataflow/src/main/resources/vible-lotus-b7535729c288.json
```

## run

```

gradle clean execute --warning-mode all \
  -Dorg.gradle.java.home=${JAVA_HOME} \
  -DmainClass=co.vible.StarterPipeline \
  -Dexec.args="--project=${PROJECT_ID} \
    --loadingBucketURL=gs://${BUCKET}/vible.example.json  \
    --tempLocation=gs://${BUCKET}/temp \
    --gcpTempLocation=gs://${BUCKET}/gcpTemp \
    --runner=DataflowRunner \
    --jobName=etl-into-bigquery-vible \
    --numWorkers=1 \
    --maxNumWorkers=2 \
    --bigQueryTablename=${PROJECT_ID}:${DATASET}.${TABLE_NAME} \
    --diskSizeGb=100 \
    --region=${REGION} \
    --workerMachineType=n1-standard-1"

```