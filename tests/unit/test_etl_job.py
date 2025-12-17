"""Unit tests for the comprehensive ETL pipeline helpers."""

import math

import pytest
from pyspark.sql import SparkSession

from src.spark.comprehensive_etl_job import (
    cleanse_company_name,
    compute_corporate_id,
    harmonize_sources,
    normalize_address,
    prepare_financial_dataframe,
    prepare_supply_chain_dataframe,
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("contexq-etl-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_cleanse_company_name_and_address():
    assert cleanse_company_name("Acme Corporation S.A.") == "acme corporation s a"
    assert cleanse_company_name(None) == ""
    assert normalize_address("Rua dos Acacias, 100 - Sao Paulo") == "rua dos acacias 100 sao paulo"


def test_compute_corporate_id_is_deterministic():
    first = compute_corporate_id("acme", "rua um")
    second = compute_corporate_id("acme", "rua um")
    third = compute_corporate_id("acme", "rua dois")
    assert first == second
    assert first != third


def test_prepare_supply_chain_dataframe_shapes_data(spark):
    raw = spark.createDataFrame(
        [
            ("seller_001", "Acme Corp", "Sao Paulo", "SP", 3, "ACME Ltda, ACME LLC"),
            ("seller_002", "Tech Vision", "Rio", "RJ", None, None),
        ],
        ["seller_id", "seller_name", "seller_city", "seller_state", "activity_places", "top_suppliers"],
    )

    prepared = prepare_supply_chain_dataframe(raw)

    assert prepared.count() == 2
    sample = prepared.where("seller_id = 'seller_001'").collect()[0]
    assert sample.canonical_name == "acme corp"
    assert sample.activity_places == 3
    assert sample.state == "SP"
    assert sample.source_system == "supply_chain"


def test_prepare_financial_dataframe_aggregates_metrics(spark):
    items = spark.createDataFrame(
        [
            ("order-1", "seller_001", 100.0, 10.0),
            ("order-2", "seller_001", 200.0, 15.0),
            ("order-3", "seller_002", 80.0, 5.0),
        ],
        ["order_id", "seller_id", "price", "freight_value"],
    )
    payments = spark.createDataFrame(
        [
            ("order-1", 120.0),
            ("order-2", 215.0),
            ("order-3", 90.0),
        ],
        ["order_id", "payment_value"],
    )
    sellers = prepare_supply_chain_dataframe(
        spark.createDataFrame(
            [
                ("seller_001", "Acme Corp", "Sao Paulo", "SP", 3, "ACME Ltda"),
                ("seller_002", "Tech Vision", "Rio", "RJ", 2, "Vision Supplies"),
            ],
            [
                "seller_id",
                "seller_name",
                "seller_city",
                "seller_state",
                "activity_places",
                "top_suppliers",
            ],
        )
    )

    financial = prepare_financial_dataframe(items, payments, sellers)

    assert financial.count() == 2
    acme = financial.where("seller_id = 'seller_001'").collect()[0]
    assert math.isclose(acme.revenue, 325.0, rel_tol=1e-6)
    assert math.isclose(acme.profit, 58.5, rel_tol=1e-6)
    assert acme.transaction_count == 2
    assert acme.source_system == "financial"


def test_harmonize_sources_merges_entities(spark):
    supply = prepare_supply_chain_dataframe(
        spark.createDataFrame(
            [
                ("seller_001", "Acme Corp", "Sao Paulo", "SP", 3, "ACME Ltda"),
                ("seller_002", "Tech Vision", "Rio", "RJ", 2, "Vision Supplies"),
            ],
            [
                "seller_id",
                "seller_name",
                "seller_city",
                "seller_state",
                "activity_places",
                "top_suppliers",
            ],
        )
    )

    items = spark.createDataFrame(
        [
            ("order-1", "seller_001", 100.0, 10.0),
            ("order-2", "seller_001", 150.0, 12.0),
        ],
        ["order_id", "seller_id", "price", "freight_value"],
    )
    payments = spark.createDataFrame(
        [("order-1", 115.0), ("order-2", 170.0)],
        ["order_id", "payment_value"],
    )
    financial = prepare_financial_dataframe(items, payments, supply)

    harmonized = harmonize_sources(supply, financial)

    assert harmonized.count() == 2
    record = harmonized.where("city = 'Sao Paulo'").collect()[0]
    assert record.activity_places == 3
    assert record.transaction_count == 2
    assert record.source_system in {"supply_chain,financial", "financial,supply_chain"}
    assert record.profit_margin > 0

