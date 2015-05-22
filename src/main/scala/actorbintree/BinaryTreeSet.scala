/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import akka.pattern.{ask}
import akka.util.Timeout
import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive  = {
    case Insert(req,id,elem) => {
      println("Tree Inserts")
      root ! Insert(req,id,elem)
      context.become(receivedOperationOutput(sender))
    }
    case Remove(req,id,elem) => println("remove value " + elem)
    case Contains(req,id,elem) => {
      println("Tree Contains")
      root ! Contains(req,id,elem)
      context.become(receivedContainsResult(sender))
    }
    case GC => context.become(garbageCollecting(self))
      self ! GC
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive =  {
    case GC =>
        println("starting collection")
        context.unbecome()
  }

  def receivedOperationOutput(aRef: ActorRef): Receive = {
    case OperationFinished(id) =>
      println("Done Operation"+id)
      aRef ! OperationFinished(id)
      context.unbecome()

  }

  def receivedContainsResult(aRef: ActorRef): Receive = {
    case ContainsResult(id,result) =>
      println("Done Contains"+id)
      aRef ! ContainsResult(id,result)
      context.unbecome()
  }

}



class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {

    case Insert(req,id,elem) =>
     println("Insert")
     if(subtrees.isEmpty){

       if (elem <= this.elem) {
         subtrees += Left -> sender
       }
       else {
         subtrees += Right -> sender
       }
       println(subtrees)

       sender ! OperationFinished(id)

     }else{
       subtrees.foreach( node => {
         node._2 ! Insert(req,id,elem)
       })
     }

    case Remove(req,id,elem) => println("remove value " + id)

    case Contains(req,id,elem) => {
      println("Contains")

      if(this.elem == elem && !removed){
        sender ! ContainsResult(id,true)
      }
      else if (!subtrees.isEmpty){

        println(subtrees)
        implicit val ec: ExecutionContext = context.dispatcher
        val futures = subtrees.map(node => {(node._2 ? Contains(req, id, elem))(3000)}).toList.map(_.mapTo[ContainsResult])
        val futureOfResults: Future[List[ContainsResult]] = Future.sequence(futures)
        val futureContain = futureOfResults map {cr => cr.map(_.result).foldLeft(true)(_ || _)}
        futureContain.map(c => sender ! ContainsResult(id,c))
      }
      else{
        sender ! ContainsResult(id,false)
      }

    }

    case CopyTo(treeNode) => println("copy to")

    case OperationFinished(id) => sender ! OperationFinished(id)

    case ContainsResult(id,result) => sender ! ContainsResult(id,result)


  }


  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = ???


}
