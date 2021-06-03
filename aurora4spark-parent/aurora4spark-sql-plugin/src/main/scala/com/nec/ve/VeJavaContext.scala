package com.nec.ve

import com.nec.aurora.Aurora

/** This is to be removed once the old functions are migrated */
final class VeJavaContext(
  val proc: Aurora.veo_proc_handle,
  val ctx: Aurora.veo_thr_ctxt,
  val lib: Long
) {}
