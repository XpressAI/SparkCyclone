package com.nec.agent

import java.lang.instrument.Instrumentation

import com.nec.aurora.Aurora
import com.nec.aurora.Aurora.veo_proc_handle
import org.slf4j.LoggerFactory
object AuroraSqlAgent {
  val logger = LoggerFactory.getLogger("AuroraSqlAgent")
  var _veo_proc: veo_proc_handle = _

  def premain(agentArgs: String, inst: Instrumentation): Unit = {
    val selectedVeNodeId = 0

    logger.info(s"Using VE node = ${selectedVeNodeId}")

    if (_veo_proc == null) {
      _veo_proc = Aurora.veo_proc_create(selectedVeNodeId)
      require(_veo_proc != null, s"Proc could not be allocated for node ${selectedVeNodeId}, got null")
      require(_veo_proc.address() != 0, s"Address for 0 for proc was ${_veo_proc}")
      logger.info(s"Opened process: ${_veo_proc}")
    }
  }
}
