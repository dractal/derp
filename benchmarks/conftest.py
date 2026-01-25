"""Pytest configuration for benchmarks."""


def pytest_configure(config):
    """Configure pytest for benchmarks."""
    config.addinivalue_line("markers", "benchmark: mark test as a benchmark")
