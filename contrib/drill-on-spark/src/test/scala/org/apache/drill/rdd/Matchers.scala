package org.apache.drill.rdd

import org.hamcrest.{Description, BaseMatcher}

abstract class GenericMatcher[T] extends BaseMatcher[T] {
  override def describeTo(description: Description): Unit = {}
}
