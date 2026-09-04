"""Unit tests for core utilities and generators."""

from src.core.config import get_s3_paths
from src.utils.generators import generate_base_customers, generate_base_sales


def test_generate_base_customers():
    """Validates customer generator logic."""
    n_customers = 10
    customers = generate_base_customers(n_customers)

    assert len(customers["id"]) == n_customers
    assert "name" in customers
    assert "city" in customers
    assert customers["city"][0] in [
        "São Paulo",
        "Rio de Janeiro",
        "Belo Horizonte",
        "Curitiba",
        "Porto Alegre",
    ]


def test_generate_base_sales():
    """Validates sales generator logic and column naming."""
    n_sales = 20
    n_customers = 5
    sales = generate_base_sales(n_sales, n_customers, (10, 100), "2024-03-29")

    assert len(sales["id"]) == n_sales
    assert "price" in sales
    assert "date" in sales
    assert all(val >= 10 for val in sales["price"])


def test_get_s3_paths():
    """Ensures S3 paths are correctly formatted."""
    paths = get_s3_paths("2024-01-01")
    assert paths["sales_landing"] == "s3://landing-zone/sales_erp/dt=2024-01-01/sales.csv"
    assert paths["sales_silver"] == "s3://silver/sales_erp/dt=2024-01-01/"
