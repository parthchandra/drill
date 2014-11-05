package org.apache.drill.rdd

import org.slf4j.LoggerFactory


object resource {
  private val logger = LoggerFactory.getLogger(getClass)

  def using[T<:{def close()}, U](resource: T)(func: T => U): U = {
    try {
      func(resource)
    } catch {
      case t:Throwable =>
        logger.error(s"error while running resource: $resource", t)
        throw t
    } finally {
      resource.close()
    }
  }

}
