# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Onyx is an enterprise AI search platform that combines document indexing, retrieval, and generative AI to provide intelligent search and chat capabilities. The backend is built with FastAPI, uses PostgreSQL and Vespa for data storage/search, Redis for caching, and Celery for background job processing.

## Architecture

The codebase is organized into several key modules:

- **onyx/**: Core application code
  - `main.py`: FastAPI application entry point
  - `configs/`: Configuration management and environment variables
  - `db/`: Database models, migrations, and utilities
  - `connectors/`: Data source integrations (50+ connectors including Slack, Google Drive, Confluence, etc.)
  - `chat/`: Chat functionality and LLM integration
  - `document_index/`: Document indexing and search using Vespa
  - `auth/`: Authentication and user management
  - `server/`: API endpoints organized by domain
  - `background/`: Celery task definitions and background processing

- **model_server/**: Separate service for ML models and embeddings
- **ee/**: Enterprise edition features (authentication, permissions, analytics)
- **alembic/**: Database migration scripts
- **scripts/**: Development and operational utilities
- **tests/**: Comprehensive test suite with unit, integration, and regression tests

## Common Development Commands

### Testing
```bash
pytest                                    # Run all tests
pytest -s tests/integration/tests/       # Run integration tests  
pytest -m slow                          # Run slow tests
pytest tests/unit/                      # Run unit tests only
pytest tests/daily/                     # Run daily regression tests
```

### Code Quality
```bash
mypy .                                   # Type checking
ruff check .                            # Linting
ruff format .                           # Code formatting
pre-commit run --all-files              # Run all pre-commit hooks
```

### Database Operations
```bash
alembic upgrade head                     # Apply all pending migrations
alembic revision --autogenerate -m "description"  # Create new migration
alembic downgrade -1                     # Rollback one migration

# Multi-tenant operations
alembic -x upgrade_all_tenants=true upgrade head  # Upgrade all tenants
alembic -x schemas=tenant_id upgrade head         # Upgrade specific tenant
```

### Development Server
```bash
uvicorn onyx.main:app --reload --host 0.0.0.0 --port 8080  # Main API server
python model_server/main.py                                # Model server (port 9000)
python scripts/dev_run_background_jobs.py                  # All Celery workers
```

### Background Jobs
Individual Celery workers can be run with:
```bash
celery -A onyx.background.celery.versioned_apps.primary worker --loglevel=INFO
celery -A onyx.background.celery.versioned_apps.beat beat  # Scheduler
```

### Container Management
```bash
./scripts/restart_containers.sh         # Restart all Docker dependencies
./scripts/docker_memory_tracking.sh     # Monitor container memory usage
```

## Test Configuration

Integration tests require:
- API server running on port 8080
- `AUTH_TYPE=basic` and `ENABLE_PAID_ENTERPRISE_EDITION_FEATURES=true`
- For some tests: mock connector server via `docker compose -f tests/integration/mock_services/docker-compose.mock-it-services.yml up -d`

## Key Dependencies

The project uses:
- **FastAPI** for the web framework
- **SQLAlchemy** for ORM with PostgreSQL
- **Vespa** for document search and retrieval
- **Celery** with Redis for background job processing
- **Alembic** for database migrations
- **LangChain** for LLM integration
- **transformers** and **sentence-transformers** for ML models

## Configuration

Environment variables are managed through:
- `onyx/configs/app_configs.py` - Main application configuration
- `shared_configs/configs.py` - Shared configuration between services

Critical configs include database connections, authentication settings, LLM provider configurations, and feature flags.

## Debugging Utilities

- `scripts/debugging/onyx_db.py` - Database inspection
- `scripts/debugging/onyx_redis.py` - Redis debugging
- `scripts/debugging/onyx_vespa.py` - Vespa search debugging
- `scripts/orphan_doc_cleanup_script.py` - Clean up orphaned documents

## Data Connectors

The system supports 50+ data sources through `onyx/connectors/`. Each connector implements the `LoadConnector` interface and handles:
- Document fetching and parsing
- Incremental updates and deletion detection  
- Permission syncing (Enterprise edition)
- Rate limiting and error handling

## Development Guidelines

- Follow existing code patterns and naming conventions
- Use type hints throughout (enforced by mypy)
- Write tests for new functionality
- Database changes require Alembic migrations
- Background jobs should be idempotent and handle failures gracefully
- API endpoints should include proper error handling and validation