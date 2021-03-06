﻿# Graph-Mining_on_MapReduce

대용량 그래프의 군집계수 분석을 위한 효율적인 분산 알고리즘 설계 및 구현

bigdata_2.11-0.1.jar 파일로 만든 후 구글 클라우드 플랫폼에서 Spark 실행

빅데이터 파일 : soc-LiveJournall.txt

- Task 1. Simple Graph

    - 그래프가 edge list file로 주어졌을떄, 중복 edge와 self-loop 제거하여 simple graph를 생성하는 Hadoop 프로그램 작성

    - 명령어 : hadoop jar bigdata_2.11-0.1.jar bigdata.Task1 -Dmapreduce=5 soc-LiveJournall.txt Task1.txt

- Task 2. Degree Computation

    - Task 1의 결과 그래프가 주어졌을 때, 각 node의 degree(이웃 node 수)를 구하는 Hadoop 프로그램을 작성

    - 명령어 : hadoop jar bigdata_2.11-0.1.jar bigdata.Task2 -Dmapreduce=5 Task1.txt Task2.txt

- Task 3. Triangle Counting
    - Task 1의 결과 그래프가 주어졌을 때, 각 node u 마다 u를 포함하는 삼각형의 수 t(u)를 계산하여 출력하는 Spark 프로그램 작성

    - 명령어 : spark-submit --num-executors 12 --class bigdata.Task3 bigdata_2.11-0.1.jar Task1.txt Task3.txt

- Task 4. Clustering Coefficient 

    - Task 2의 결과와 Task3의 결과가 주어졌을 때, 각 node u의 군집계수 cc(u)를 계산하여 출력하는 Spark 프로그램 작성

    - 명령어 : spark-submit --num-executors 12 --class bigdata.Task4 bigdata_2.11-0.1.jar Task2.txt Task3.txt Task4.spark.res

