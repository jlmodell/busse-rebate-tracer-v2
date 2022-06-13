from pymongo import MongoClient
from ..constants.database_constants import BUSSE_REBATE_TRACES, BUSSE_PRICING
from ..constants.environ import MONGODB_URI


__rebate_db__ = MongoClient(MONGODB_URI)[BUSSE_REBATE_TRACES]
__pricing_db__ = MongoClient(MONGODB_URI)[BUSSE_PRICING]

# exports

tracings = __rebate_db__.tracings
sched_data = __rebate_db__.sched_data
contracts = __rebate_db__.contracts
confidence_check = __rebate_db__.confidence_check

data_warehouse = __rebate_db__.data_warehouse
discrepancies = __rebate_db__.discrepancies

pricing_contracts = __pricing_db__.contract_prices

# /exports
