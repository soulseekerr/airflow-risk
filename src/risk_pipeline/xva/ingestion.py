"""Compatibility imports for the normalized, independently sourced input-feed workflow."""

from risk_pipeline.xva.feeds import ingest_submissions, parse_payload, validate_ingestion

__all__ = ["ingest_submissions", "parse_payload", "validate_ingestion"]
