"""Shared utility functions for synthetic data generation."""

import numpy as np


def generate_base_customers(n_customers):
    """Generates a base dictionary for customer data."""
    return {
        "id": range(1001, 1001 + n_customers),  # Start from 1001 to avoid ID 0 issues
        "name": [f"Customer_{i}" for i in range(1, n_customers + 1)],
        "email": [f"customer_{i}@example.com" for i in range(1, n_customers + 1)],
        "city": np.random.choice(
            [
                "São Paulo",
                "Rio de Janeiro",
                "Belo Horizonte",
                "Curitiba",
                "Porto Alegre",
            ],
            n_customers,
        ),
        "state": np.random.choice(["SP", "RJ", "MG", "PR", "RS"], n_customers),
    }


def generate_base_sales(n_sales, n_customers, amount_range, base_date, customer_id_offset=0):
    """Generates a base dictionary for sales data with requested names."""
    low_amount, high_amount = amount_range
    return {
        "id": range(1, n_sales + 1),
        "customer_id": np.random.randint(
            1001, 1001 + n_customers + customer_id_offset, size=n_sales
        ),
        "price": np.random.uniform(low_amount, high_amount, size=n_sales),
        "product": np.random.choice(
            ["Laptop", "Smartphone", "Tablet", "Monitor", "Keyboard"], n_sales
        ),
        "date": [base_date] * n_sales,
    }
