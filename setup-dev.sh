#!/bin/bash

# Install development requirements
pip install -r requirements/dev.txt

# Install pre-commit hooks
pre-commit install

# Initial code formatting
echo "Running initial code formatting..."
black .
isort .

# Run initial checks
echo "Running code quality checks..."
flake8
mypy .
pylint apps/
bandit -r .

echo "Running tests..."
pytest

echo "Setup complete! Your development environment is ready."
echo ""
echo "Available commands:"
echo "black .              # Format code"
echo "isort .             # Sort imports"
echo "flake8              # Style checking"
echo "mypy .              # Type checking"
echo "pylint apps/        # Code analysis"
echo "bandit -r .         # Security checks"
echo "pytest              # Run tests with coverage" 