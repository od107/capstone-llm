FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.5.1-hadoop-3.3.6-v1

USER 0
ENV PYSPARK_PYTHON python3
WORKDIR /opt/spark/work-dir

RUN python3 -m pip install --upgrade pip

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt --no-cache-dir

COPY . .
RUN pip install .

