class BobVaultLogsProcessor:
    _chainid: str

    def __init__(self, chainid: str):
        self._chainid = chainid

    def get_chainid(self) -> str:
        return self._chainid

    def __repr__(self) -> str:
        return type(self).__name__

    def pre(self, snapshot: dict) -> bool:
        pass

    def process(self, trade: dict) -> bool:
        pass

    def post(self) -> bool:
        pass

    def monitor(self) -> bool:
        pass