import logging

log = logging.getLogger("makegis")


def capture_logs(stream, log_prefix: str):
    if stream is not None:
        for line in stream:
            msg = f"[{log_prefix}] {line.strip()}"
            lower_line = line.lower()
            if "error" in line.lower():
                log.error(msg)
            elif "warn" in lower_line:
                log.warning(msg)
            else:
                log.info(msg)
