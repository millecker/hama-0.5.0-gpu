package org.apache.hama.pipes.protocol;

/**
 * The integer codes to represent the different messages. These must match the
 * C++ codes or massive confusion will result.
 */
public enum MessageType {
  START(0), SET_BSPJOB_CONF(1), SET_INPUT_TYPES(2), RUN_SETUP(3), RUN_BSP(4), RUN_CLEANUP(
      5), READ_KEYVALUE(6), WRITE_KEYVALUE(7), GET_MSG(8), GET_MSG_COUNT(9), SEND_MSG(
      10), SYNC(11), GET_ALL_PEERNAME(12), GET_PEERNAME(13), GET_PEER_INDEX(
      14), GET_PEER_COUNT(15), GET_SUPERSTEP_COUNT(16), REOPEN_INPUT(17), CLEAR(
      18), CLOSE(19), ABORT(20), DONE(21), TASK_DONE(22), REGISTER_COUNTER(23), INCREMENT_COUNTER(
      24), SEQFILE_OPEN(25), SEQFILE_READNEXT(26), SEQFILE_APPEND(27), SEQFILE_CLOSE(
      28), PARTITION_REQUEST(29), PARTITION_RESPONSE(30);

  final int code;

  MessageType(int code) {
    this.code = code;
  }
}