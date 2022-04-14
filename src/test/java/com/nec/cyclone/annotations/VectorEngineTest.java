package com.nec.cyclone.annotations;

import java.lang.annotation.*;
import org.scalatest.TagAnnotation;

/*
  Define an annotation to decorate a test class:
    https://www.scalatest.org/scaladoc/3.2.11/org/scalatest/Tag.html
*/
@TagAnnotation
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface VectorEngineTest {}
