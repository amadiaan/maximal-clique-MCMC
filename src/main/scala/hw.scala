import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.Graph

import scala.util.Random
import org.jgrapht._
import org.jgrapht.graph._
import org.jgrapht.alg._

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object Hi {

  val cliqueList: List[Graph[String,Int]] = null
  val conf = new SparkConf().setMaster("local[*]").setAppName("GraphX")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) = {
    println("I am working")
    val graph = GraphLoader.edgeListFile(sc, "smalGraph.txt")

    //intitializeState(graph)
    val randomVertex = graph.pickRandomVertex()
    val tmp = graph.subgraph(epred = e => e.srcId == 4).vertices
    tmp.repartition(1).saveAsTextFile("vertex")
    val nextGraph = graph.subgraph(epred = e => e.srcId == 1 && e.srcId == 2)

  }

  def isComplete[VD](g: Graph[VD,Int]) : Boolean = {
    val numberOfNode = g.vertices.count()
    val numberOfEdges = g.edges.count()
    if (numberOfEdges == (numberOfNode*(numberOfNode-1))) true else false
  }

  def intitializeState[VD: ClassTag](g: Graph[VD,Int]) = {
    val randomVertex = g.pickRandomVertex()
    g.subgraph(epred = e => e.srcId == randomVertex)
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

  def gc()={

    /*val neighbotInfo = graph.collectNeighborIds(EdgeDirection.Either).map(x => (x._1 , x._2(0) , x._2(1)))
    neighbotInfo.repartition(1).saveAsTextFile("neighborInfo")*/

    /*val strongConnComp = graph.stronglyConnectedComponents(10)
      .vertices
      .map(_.swap)
      .groupByKey
      .map(_._2).collect()
    println( "strongly connected comp : " + strongConnComp(0))

    println(isComplete(graph))
    println("number of edges : " + graph.edges.count())
    println("number of vertices : " + graph.vertices.count())
    val inD = graph.inDegrees
    inD.foreach(x => println(x))


    //insert itr = 0 and first clique to the DF
    var cliqueDF = sqlContext.createDataFrame( Seq(CliqueSet(0 , List(1))) )
      .toDF("iteration" , "cliqueVertexSet")
    cliqueDF.show()

    for(i <- 1 to numberOfIteration){
      //val currentState = select from DF
      var stateGraph: Graph[Int,Int] = DataFrameVertexSetToCompleteGraph(cliqueDF , i)
      stateGraph.vertices.foreach(println)
      stateGraph.edges.foreach(println)
      //state.vertices is key value pair which the key is a vertex property which is by default 1.
      //we dont need that here
      var verticesIdInState = stateGraph.vertices.map(x=>x._1)
      // if we don't convert it to array (use collect instead of Array)
      // after map and calling isEdgeExist method we will get NullPointerexeption. I have to solve this problem
      var verticesIdInStateArray = verticesIdInState.collect()
      var randomVertex = graph.pickRandomVertex()
      var randomVertexConnectInfoToVerticesInState = verticesIdInStateArray.map(
        vertex => (vertex , isEdgeExist2(vertex , randomVertex , graph))
      )
      var verticesInStateConnectToRandomVertex = randomVertexConnectInfoToVerticesInState.filter(x => x._2 == true)
      verticesInStateConnectToRandomVertex.foreach(println)
      //if true we accept the random vertex
      var RandomVertexPlusStateIsNewState: Boolean =
        verticesInStateConnectToRandomVertex.size == verticesIdInState.count()
      //now we have to union the random vertex and the old state and create a new state
      //lets create another graph for the chain of states and call it MarKovGraph
      if (RandomVertexPlusStateIsNewState){
        //union random Vertex and State
        //val zzz = UpdateState(randomVertex , stateGraph).vertices.collect.map(x=>x._1.toInt).toList
        var cliqueDF2  = sqlContext.createDataFrame( Seq(CliqueSet(i
          , UpdateState(randomVertex , stateGraph).vertices.collect.map(x=>x._1.toInt).toList)) )
          .toDF("iteration" , "cliqueVertexSet").unionAll(cliqueDF)
        cliqueDF = cliqueDF2.unionAll(cliqueDF)
        cliqueDF.show()
        cliqueDF.coalesce(1).write.json("cliqueSet--" + i.toString)
      }



    //insert itr = 0 and first clique to the DF
    var cliqueDF = sqlContext.createDataFrame( Seq(CliqueSet(0 , List(1))) )
      .toDF("iteration" , "cliqueVertexSet")
    cliqueDF.show()


 def DataFrameVertexSetToCompleteGraph(df: DataFrame , iteration:Int): Graph[Int,Int] = {
    val stateVertexSet = df.filter(df("iteration").===(iteration-1)).select("cliqueVertexSet")
     val stateVertexList = stateVertexSet.takeAsList(2).get(0).getAs[mutable.WrappedArray[Int]](0).toList
    CompleteGraph(stateVertexList)
  }


    */
  }
}