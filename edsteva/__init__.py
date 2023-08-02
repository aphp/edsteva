__version__ = "0.2.4"


import importlib
import os
import sys
import time
from distutils.version import LooseVersion
from pathlib import Path
from typing import List, Tuple

import pyarrow
import pyspark
from loguru import logger
from pyspark import SparkContext
from pyspark.sql import SparkSession

logger.remove()
logger.add(sys.stderr, level="INFO")


def koalas_options() -> None:
    """
    Set necessary options to optimise Koalas
    """

    # Reloading Koalas to use the new configuration
    ks = sys.modules.get("databricks.koalas", None)

    if ks is not None:
        importlib.reload(ks)

    else:  # pragma: no cover
        import databricks.koalas as ks

    ks.set_option("compute.default_index_type", "distributed")
    ks.set_option("compute.ops_on_diff_frames", True)


def set_env_variables() -> None:
    # From https://github.com/databricks/koalas/blob/master/databricks/koalas/__init__.py
    if LooseVersion(pyspark.__version__) < LooseVersion("3.0"):
        if LooseVersion(pyarrow.__version__) >= LooseVersion("0.15"):
            os.environ["ARROW_PRE_0_15_IPC_FORMAT"] = "1"

    if LooseVersion(pyarrow.__version__) >= LooseVersion("2.0.0"):  # pragma: no cover
        os.environ["PYARROW_IGNORE_TIMEZONE"] = "0"


def improve_performances(
    to_add_conf: List[Tuple[str, str]] = None,
    quiet_spark: bool = True,
) -> Tuple[SparkSession, SparkContext, SparkSession.sql]:
    """
    (Re)defines various Spark variable with some configuration changes
    to improve performances by enabling Arrow
    This has to be done
    - Before launching a SparkCOntext
    - Before importing Koalas
    Those two points are being taken care on this function.
    If a SparkSession already exists, it will copy its configuration before
    creating a new one

    Returns
    -------
    Tuple of
    - A SparkSession
    - The associated SparkContext
    - The associated ``sql`` object to run SQL queries
    """

    # Check if a spark Session is up
    global spark, sc, sql

    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    if quiet_spark:
        sc.setLogLevel("ERROR")

    conf = sc.getConf()

    # Synchronizing TimeZone
    tz = os.environ.get("TZ", "UTC")
    os.environ["TZ"] = tz
    time.tzset()

    if to_add_conf is None:
        to_add_conf = []

    to_add_conf.extend(
        [
            ("spark.app.name", f"{os.environ.get('USER')}_scikit"),
            ("spark.sql.session.timeZone", tz),
            ("spark.sql.execution.arrow.enabled", "true"),
            ("spark.sql.execution.arrow.pyspark.enabled", "true"),
        ]
    )

    for key, value in to_add_conf:
        conf.set(key, value)

    # Stopping context to add necessary env variables
    sc.stop()
    spark.stop()

    set_env_variables()

    spark = SparkSession.builder.enableHiveSupport().config(conf=conf).getOrCreate()

    sc = spark.sparkContext

    if quiet_spark:
        sc.setLogLevel("ERROR")

    sql = spark.sql

    koalas_options()

    return spark, sc, sql


CACHE_DIR = Path.home() / ".cache" / "edsteva"
