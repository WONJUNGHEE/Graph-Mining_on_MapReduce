package bigdata;

import org.apache.spark.{SparkConf, SparkContext}

object Task3 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Task3");
    val sc = new SparkContext(conf);

    val input = args(0);
    val output = args(1);
    val txt = sc.textFile(args(0)).repartition(120);

    val data =txt.map{x=>(x.split("\t")(0),x.split("\t")(1))};
    val zero=data.map{x=>((x._1,x._2),0)};
    //degree 구하기
    val degree =data.flatMap(_.productIterator).map(x=>(x.toString,1)).reduceByKey(_+_);
    //엣지 에 degree 정보 입력
    val joinn= data.join(degree).map{x=>(x._2._1,(x._1,x._2._2))}.join(degree).map{case(a,((b,c),d))=>(b,a,c,d)}.filter{case(a,b,c,d)=>c>1 ||d>1};
    //degree 낮은 순으로 edge 정렬
    val wedge= joinn.map{case(u,v,du,dv)=>if(du<dv ||(du==dv && u<v)) (u,v) else (v,u)};
    // 낮은 degree 기준으로 wedge 구현 뒤에 노드는 숫자 크기로 정렬
    val realwedge= wedge.groupByKey().filter(x=>x._2.size>1).flatMap{x=>for(v1 <- x._2;v2<-x._2;if (v1<v2)) yield  ((v1,v2),x._1) }
    //wedge랑 초기 edge랑 join
    val res=realwedge.join(zero).map{case((x,y),(z,k))=>(z,x,y)}.flatMap(_.productIterator).map{x=>(x,1)}.reduceByKey(_+_);
    val out=res.map(x=>x._1+"\t"+x._2).saveAsTextFile(output);

  }

}