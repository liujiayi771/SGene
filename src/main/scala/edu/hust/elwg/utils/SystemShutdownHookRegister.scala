package edu.hust.elwg.utils

import scala.collection.mutable.{Map => MMap}

object SystemShutdownHookRegister {
  lazy val hooks: MMap[String, (() => Unit)] = MMap[String, (() => Unit)]()
  val activate: Boolean = sys.addShutdownHook(
    SystemShutdownHookRegister.doShutdown()).isAlive

  /**
    * @param name name of the function
    * @param exec registed function
    * @return true for a new one , and false for replaced another
    */
  def register(name: String, exec: () => Unit): Boolean =
    hooks.synchronized(hooks.put(name, exec).isEmpty)

  /**
    * @param name deregisted name
    * @return true for some function removed
    */
  def deregister(name: String): Boolean =
    hooks.synchronized(hooks.remove(name).isDefined)

  def doShutdown(): Unit =
    hooks.synchronized(hooks.foreach(_._2()))
}
