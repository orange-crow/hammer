import numpy as np

# https://clickhouse.com/docs/en/sql-reference/data-types
CH2PANDAS = {
    "Bool": np.bool,
    "FixedString": str,
    "String": str,
    "Int8": np.int8,
    "Int16": np.int16,
    "Int32": np.int32,
    "Int64": np.int64,
    "Int128": int,
    "Int256": int,
    "UInt8": np.uint8,
    "UInt16": np.uint16,
    "UInt32": np.uint32,
    "UInt64": np.uint64,
    "UInt128": int,
    "UInt256": int,
    "Float16": np.float16,
    "Float32": np.float32,
    "Float64": np.float64,
    "BFloat16": np.float16,
    "DateTime": "datetime",
    "Date": "datetime",
    "DateTime64(N)": "datetime",
    "Enum8": np.int8,
    "Enum16": np.int16,
}
