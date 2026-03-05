class FailedNodeRun(Exception):
    """Failed node run"""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)
