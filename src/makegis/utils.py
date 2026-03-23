import logging

log = logging.getLogger("makegis")


def capture_logs(stream, log_prefix: str):
    # Flag to indicate a traceback was detected in the logs
    traceback = False

    if stream is not None:
        for line in stream:
            msg = f"[{log_prefix}] {line.rstrip()}"
            lower_line = line.lower()
            if traceback or "error" in line.lower():
                log.error(msg)
            elif "warn" in lower_line:
                log.warning(msg)
            elif "traceback (most recent call last)" in lower_line:
                traceback = True
                log.error(msg)
            else:
                log.info(msg)
