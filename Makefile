.PHONY: install rebuild test lint run clean

# Create (or update) the virtual environment and install dependencies
install:
	@echo "Installing dependencies and creating local .venv..."
	poetry install

# Remove the existing virtual environment and reinstall dependencies
rebuild:
	@echo "Removing local virtual environment..."
	poetry env remove $(shell poetry env info --path) || true
	@echo "Rebuilding virtual environment..."
	poetry install

# Run tests using pytest
test:
	@echo "Running tests..."
	poetry run pytest

# Run linters (black and isort) in check mode
lint:
	@echo "Checking code formatting with Black..."
	poetry run black --check .
	@echo "Checking import order with isort..."
	poetry run isort --check .

# Run the dbtlens CLI command (change arguments as needed)
run:
	@echo "Running dbtlens CLI..."
	poetry run dbtlens

# Clean the virtual environment (use with caution)
clean:
	@echo "Removing local .venv folder..."
	rm -rf .venv
