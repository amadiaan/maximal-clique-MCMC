/**
  * Created by ahmad on 1/8/17.
  */

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

object subgraphTest {

  val conf = new SparkConf().setMaster("local[*]").setAppName("subgraph")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {


    val vertices = sc.makeRDD(Seq(
      (1L , "Ann"),(2L , "Bill"),(3L , "Charles"),(4L , "Dianne")))
    val edges = sc.makeRDD(Seq(
      Edge(1L , 2L , "is-friend-with") , Edge(1L , 3L , "is-friend-with")
      , Edge(4L , 1L , "has-bocked") , Edge(2L , 3L , "has-blocked")
      , Edge(3L , 4L , "has-blocked")))

    val originalGraph = Graph(vertices , edges)

    val subgraph = originalGraph.subgraph(et => et.attr == "is-friend-with")
    subgraph.vertices.foreach(println)
    removeSingletones(subgraph).vertices.foreach(println)
  }

  def removeSingletones[VD: ClassTag , ED: ClassTag](g: Graph[VD , ED]) = {
    Graph(g.triplets.map(et => (et.srcId , et.srcAttr))
        .union(g.triplets.map(et => (et.dstId,et.dstAttr)))
        .distinct(), g.edges
    )
  }

  def UpdateState(randomVertex: VertexId , state: Graph[Int,Int])  = {

    val stateVertices = state.vertices.map(_._2)
    val newEdgesRDD = stateVertices.map(x => Edge(randomVertex , x , 1))

    val randVerToAllVerInState : Graph[Int,Int] =  Graph(
      sc.makeRDD(Seq((randomVertex , 1))),
      newEdgesRDD)

      mergeGraphs(randVerToAllVerInState , state)
  }

  def mergeGraphs(g1: Graph[Int , Int] , g2: Graph[Int,Int]) = {
    val v = g1.vertices.map(_._2).union(g2.vertices.map(_._2)).distinct().zipWithIndex()
    def edgesWithNewVertexIds(g: Graph[Int , Int]) =
      g.triplets
      .map(et => (et.srcAttr , (et.attr,et.dstAttr)))
      .join(v)
      .map(x => (x._2._1._2, (x._2._1._2 , x._2._2 , x._2._1._1)))
      .join(v)
      .map(x => new Edge(x._2._1._1 , x._2._2 , x._2._1._2))
    Graph(v.map(_.swap) , edgesWithNewVertexIds(g1).union(edgesWithNewVertexIds(g2)))
  }
}