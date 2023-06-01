from web3 import Web3
from datetime import datetime

MAX_INT = (2 ** 128) - 1
ONE_DAY = 24 * 60 * 60
TWO_POW_96 = 2 ** 96

ONE_ETHER = 10 ** 18

BOB_TOKEN_ADDRESS = Web3.toChecksumAddress("0xB0B195aEFA3650A6908f15CdaC7D92F8a5791B0B")
ZERO_ADDRESS = Web3.toChecksumAddress("0x0000000000000000000000000000000000000000")

ABI_DIR = 'abi'

ZERO_DATETIME = datetime.fromtimestamp(0)
