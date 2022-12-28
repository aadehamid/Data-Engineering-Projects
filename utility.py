
from psycopg2.extensions import register_adapter, AsIs
import numpy as np
# This cell registers the numpy data types with psycopg2
# to avoid the error: "TypeError: can't adapt type <numpy data type>"
def addapt_numpy_float64(numpy_float64):
    """A function to adapt numpy float64 to psycopg2

    Args:
        numpy_float64 (np.float64): Numpy data type to register with psycopg2

    Returns:
        None:
    """
    return AsIs(numpy_float64)

def addapt_numpy_int64(numpy_int64):
    """A function to addapt numpy int64 to psycopg2

    Args:
        numpy_int64 (np.int64): Numpy data type to register with psycopg2

    Returns:
        None:
    """
    return AsIs(numpy_int64)

def addapt_numpy_float32(numpy_float32):
    """A function to addapt numpy int64 to psycopg2

    Args:
        numpy_float32 (np.float32): Numpy data type to register with psycopg2

    Returns:
        None:
    """
    return AsIs(numpy_float32)

def addapt_numpy_int32(numpy_int32):
    """A function to addapt numpy int64 to psycopg2

    Args:
        numpy_int32 (np.int32): Numpy data type to register with psycopg2

    Returns:
        None:
    """
    return AsIs(numpy_int32)

def addapt_numpy_array(numpy_array):
    """A function to addapt numpy int64 to psycopg2

    Args:
        numpy_array : Numpy data type to register with psycopg2

    Returns:
        None:
    """
    return AsIs(tuple(numpy_array))

if __name__ == "__main__":
    # register the numpy data types with psycopg2
    register_adapter(np.ndarray, addapt_numpy_array)
    register_adapter(np.float64, addapt_numpy_float64)
    register_adapter(np.int64, addapt_numpy_int64)
    register_adapter(np.float32, addapt_numpy_float32)
    register_adapter(np.int32, addapt_numpy_int32)
