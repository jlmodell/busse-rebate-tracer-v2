# database constants
BUSSE_REBATE_TRACES = "BUSSE_REBATE_TRACES"
BUSSE_PRICING = "BUSSE_PRICING"
# collection constants
ROSTERS = "ROSTERS"
ARCHIVED_ROSTERS = "ARCHIVED_ROSTERS"
TRACINGS = "TRACINGS"
SCHED_DATA = "SCHED_DATA"
CONTRACTS = "CONTRACTS"
CONFIDENCE_CHECK = "CONFIDENCE_CHECK"
DATA_WAREHOUSE = "DATA_WAREHOUSE"
DISCREPANCIES = "DISCREPANCIES"
PRICING_CONTRACTS = "PRICING_CONTRACTS"

# databases
DATABASES = {BUSSE_REBATE_TRACES: "busserebatetraces", BUSSE_PRICING: "bussepricing"}

# collections
BUSSE_REBATE_TRACES_COLLECTIONS = {
    ROSTERS: "roster",
    ARCHIVED_ROSTERS: "archived_roster",
    TRACINGS: "tracings",
    SCHED_DATA: "sched_data",
    CONTRACTS: "contracts",
    CONFIDENCE_CHECK: "confidence_check",
    DATA_WAREHOUSE: "data_warehouse",
    DISCREPANCIES: "discrepancies",
}

BUSSE_PRICING_COLLECTIONS = {
    PRICING_CONTRACTS: "contract_prices",
}


# indexes
ATLAS_SEARCH_INDEX_NAME = "find_license"

# redis
REDIS_QUEUE = "queue:raw_rebate_data"
