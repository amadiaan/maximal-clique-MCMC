/**
  * Created by ahmad on 1/10/17.
  */
import org.apache.spark.graphx._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scala.collection.mutable.WrappedArray._
import org.apache.spark.rdd.RDD

import scala.util._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.util.Random


object RandomWalk {

  val conf = new SparkConf().setMaster("local[*]").setAppName("graph")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  //case class MarkovState(stateId: VertexId , clique: Graph[Int,Int])
  case class CliqueSet(iteration: Int , cliqueVertexSet:Seq[Int])
  val numberOfIteration = 1
  val graph = GraphLoader.edgeListFile(sc , "samllGraph")
  val initialState = GraphLoader.edgeListFile(sc , "smallGraph2")
  var marKovTupleList : List[(Int, Graph[Int,Int])] = Nil

  def main(args: Array[String]): Unit = {



    marKovTupleList = marKovTupleList :+ ( 0 , initialState)
    marKovTupleList.foreach(x => println(x))

    for( i <- 1 to 10){
      marKovTupleList = marKovTupleList :+ ( i , nextState(initialState))
      var out = marKovTupleList.map(x=> x._2)
      out.foreach( x => x.vertices.collect().foreach(println))
      val itr = sc.parallelize(marKovTupleList)
      //out.foreach(x => (x.vertices).coalesce(1).saveAsTextFile("state---"+ x.toString +"---" + i.toString))
      val forPrint = marKovTupleList.map(x => (x._2.vertices.collect()))
      forPrint.foreach(_.foreach(println))
    }

    //sc.textFile("state*").coalesce(1).saveAsTextFile("Final-states")
    List(1,2,3,4,5,6,7,8,9,10).foreach( x => marKovTupleList = marKovTupleList :+ ( x , nextState(initialState)) )
    marKovTupleList.foreach(x => println(x))

  }

  def nextState(stateGraph: Graph[Int,Int]): Graph[Int,Int] = {
    var newState = stateGraph
    var verticesIdInState = stateGraph.vertices.map(x=>x._1)
    var verticesIdInStateArray = verticesIdInState.collect()
    var randomVertex = graph.pickRandomVertex()

    if (stateGraph.vertices.collect().map(_._1).contains(randomVertex)){

      var newVertices = stateGraph.vertices.map(_._1).distinct().filter(x => !x.equals(randomVertex)).collect()
      var newVerticesList: List[Long] = List.fromArray(newVertices)
      newState = CompleteGraph(newVerticesList)
    } else {
      var randomVertexConnectInfoToVerticesInState = verticesIdInStateArray.map(
        vertex => (vertex , isEdgeExist2(vertex , randomVertex , graph))
      )
      var verticesInStateConnectToRandomVertex = randomVertexConnectInfoToVerticesInState.filter(x => x._2 == true)
      verticesInStateConnectToRandomVertex.foreach(println)
      //if true we accept the random vertex
      var RandomVertexPlusStateIsNewState: Boolean =
        verticesInStateConnectToRandomVertex.size == verticesIdInState.count()
      if (RandomVertexPlusStateIsNewState){
        newState = UpdateState(randomVertex , stateGraph)
      }
    }
    newState
  }

  def isEdgeExist2(vertexSourceId: VertexId  , vertexDesId: VertexId , graph: Graph[Int,Int]): Boolean = {
    !graph.edges.filter{edge =>
      edge.srcId == vertexSourceId && edge.dstId == vertexDesId
    }.isEmpty()
  }

  def UpdateState(randomVertex: VertexId , state: Graph[Int,Int])  = {

    val stateVertices = state.vertices.map(_._2)
    val newEdgesRDD = stateVertices.map(x => Edge(randomVertex , x , 1))

    val randVerToAllVerInState : Graph[Int,Int] =  Graph(
      sc.makeRDD(Seq((randomVertex , 1))),
      newEdgesRDD)

    mergeGraphs(randVerToAllVerInState , state)
  }

  def mergeGraphs(g1: Graph[Int , Int] , g2: Graph[Int,Int]): Graph[Int,Int] = {
    val v = g1.vertices.map(_._2).union(g2.vertices.map(_._2)).distinct().zipWithIndex()
    def edgesWithNewVertexIds(g: Graph[Int , Int]) =
      g.triplets
        .map(et => (et.srcAttr , (et.attr,et.dstAttr)))
        .join(v)
        .map(x => (x._2._1._2, (x._2._2 , x._2._1._1)))
        .join(v)
        .map(x => new Edge(x._2._1._1, x._2._2 , x._2._1._2))
    Graph(v.map(_.swap) , edgesWithNewVertexIds(g1).union(edgesWithNewVertexIds(g2)))
  }

  def CompleteGraph(vertices: List[Long]): Graph[Int,Int] = {
    val verticesRDD = sc.parallelize(vertices.map( x => (x.toLong , 1)))
    val edgesRDD = sc.parallelize( Permutations(vertices)
      .filter{ x => x.length ==2}
        .map { x => Edge(x(0).toLong , x(1).toLong , 1)})
    Graph(verticesRDD , edgesRDD.distinct())
  }

  def Permutations[T](lst: List[T]): List[List[T]] = lst match {
    case Nil => List(Nil)
    case x :: xs => Permutations(xs) flatMap { perm =>
      ((0 to xs.length) flatMap { num =>
      List(perm, (perm take num) ++ List(x) ++ (perm drop num))
    })
    }
  }
}