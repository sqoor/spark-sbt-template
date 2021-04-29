# Spark sbt template

This sbt template enables you to create a new spark project 

## Package

To package your project:
```bash
sbt assembly
```

## Run

spark-submit [options] <app jar | python file | R file> [app arguments]   

To run your project locally:
```
JAR_PATH=$(pwd)/target/scala-2.12/spark-sbt-template-assembly-1.0.jar
spark-submit --master=local[*] --deploy-mode client --class App $JAR_PATH
```