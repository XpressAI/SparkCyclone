/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.sparkcyclone.util

import org.apache.commons.lang3.reflect.FieldUtils
import scala.collection.mutable.Set
import scala.language.dynamics
import sun.misc.Unsafe

object ReflectionOps {
  implicit def getUnsafe: Unsafe = {
    val tmp = classOf[Unsafe].getDeclaredField("theUnsafe")
    tmp.setAccessible(true)
    tmp.get(null).asInstanceOf[Unsafe]
  }

  implicit class ExtendedAnyRef(obj: AnyRef) {
    def readPrivate: PrivateReader = {
      new PrivateReader(obj)
    }
  }

  final class PrivateReader(val obj: AnyRef) extends Dynamic {
    def selectDynamic(name: String): PrivateReader = {
      val clazz = obj.getClass
      val field = FieldUtils.getAllFields(clazz).find(_.getName == name) match {
        case Some(f) => f
        case None =>
          throw new NoSuchFieldException(
            s"Class ${clazz} does not appear to have the field '${name}'"
          )
      }
      field.setAccessible(true)
      new PrivateReader(field.get(obj))
    }
  }
}
