FROM apache/spark:3.5.0

USER root
WORKDIR /app

RUN mkdir -p \
    /data/datalake/bronze/hubs \
    /data/datalake/bronze/links \
    /data/datalake/bronze/satellites \
    /data/datalake/silver \
    /data/datalake/gold/powerbi && \
    chmod -R 777 /data/datalake

ADD https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.6.1.jre11/mssql-jdbc-12.6.1.jre11.jar \
    /opt/spark/jars/mssql-jdbc.jar

ADD https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3-bundle_2.12/1.1.0/hudi-spark3-bundle_2.12-1.1.0.jar \
    /opt/spark/jars/hudi-spark-bundle.jar

COPY pipeline.py /app/pipeline.py

CMD ["/opt/spark/bin/spark-submit","--master","local[*]","--driver-memory","2g","--conf","spark.sql.shuffle.partitions=4","--conf","spark.sql.adaptive.enabled=true","--jars","/opt/spark/jars/hudi-spark-bundle.jar,/opt/spark/jars/mssql-jdbc.jar","/app/pipeline.py"]
