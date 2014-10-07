package org.apache.drill.rdd

object resource {

  def using[T<:{def close()}](resource: T)(func: T=>Unit): Unit = {
    try {
      func(resource)
    } finally {
      if (resource!=null) {
        resource.close()
      }
    }
  }

}
