# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

import random
import time
from enum import Enum
from typing import Optional

from dba.lib import log_lib as log
from osc.lib.error import OSCError


# This class represents an instance running the Catchup process. The fast catchup tool
# is a binary that is responsible to get the new writes on the old table to the new
# table (after transformation). The fast catchup tool also maintains a checkpoint
# table here https://fburl.com/code/difmttwp to keep track of the last processed GTID.
# Using this table, we can have another method to see the last processed gtid during
# catchup and resume from that instead of starting from the beginning.
class OscCatchupTool:
    """
    DOC_STRING
    """

    class Status(Enum):
        NOT_STARTED = 0
        RUNNING = 1
        COMPLETED = 2
        FAILED = 3

    def __init__(
        self, database_name: str, old_table_name: str, new_table_name: str, ddl_sql: str
    ) -> None:
        self.database_name = database_name
        self.old_table_name = old_table_name
        self.new_table_name = new_table_name
        self.ddl_sql = ddl_sql

    @classmethod
    def _generate_new_run_id(cls) -> int:
        """
        Returns the current time plus a random number as a 64-bit integer.
        """
        timestamp = time.time()
        timestamp_nanos = timestamp * 10**9
        return int(timestamp_nanos) + random.SystemRandom().randint(0, 10**9 - 1)

    @classmethod
    def get_catchup_tool_parent_dir(cls, job_id: int) -> str:
        """
        Returns the top level parent directory for the catchup tool binary.
        """
        return f"/tmp/catchup_tool/{job_id}"

    # Boostrap the catchup job by creating all the necessary table, directory
    # and monitor the status of the catchup job
    @classmethod
    def start_catchup_job(
        cls,
        database_name: str,
        old_table_name: str,
        new_table_name: str,
        ddl_sql: str,
        start_gtid_set: str,
    ) -> None:
        log.info(
            f"Attempt to catchup {old_table_name} to {new_table_name} "
            f"with the ddl_sql = {ddl_sql} and gtid_set = {start_gtid_set}"
        )
        raise OSCError("NOT_IMPLEMENTED_EXCEPTION")

    @classmethod
    def stop_catchup_job(cls) -> None:
        raise OSCError("NOT_IMPLEMENTED_EXCEPTION")

    # This is a blocking method to write the ending signal to the end
    # of the stream. This is important in case of catchup without any
    # ending GTID
    @classmethod
    def write_stop_catchup_job_signal(cls) -> None:
        raise OSCError("NOT_IMPLEMENTED_EXCEPTION")

    # This is a async method to catchup to a specific GTID set if the
    # ending_gtid_set is populated. In OSC, we will only use this method
    # prior to the checksum phase, after that we the catchup will happen
    # indefinitely until it is stopped.
    @classmethod
    def async_catchup_table_to_gtid_set(cls, ending_gtid_set: str = "") -> None:
        log.info(f"Attempt to catchup to gtid set {ending_gtid_set}")
        raise OSCError("NOT_IMPLEMENTED_EXCEPTION")

    # This is a blocking method to wait for the catchup job to complete
    # running. This has to also periodically check for abort flag if
    # there is any to stop the catchup job
    @classmethod
    def wait_for_catchup_job_to_finish(cls, timeout: Optional[int] = None) -> str:
        log.info("Waiting for catchup job to finish")
        raise OSCError("NOT_IMPLEMENTED_EXCEPTION")
