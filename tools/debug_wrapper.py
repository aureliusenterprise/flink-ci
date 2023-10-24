import logging
import os
import runpy
import sys

import debugpy

debugpy.listen(("localhost", 5678))

if os.getenv("WAIT_FOR_DEBUGGER"):
    logging.info("Waiting for debugger to attach...")
    debugpy.wait_for_client()

# The path to the Flink Python script is expected to be the first argument.
script_path = sys.argv[1]
runpy.run_path(script_path, run_name="__main__")
