"""file_profiler — Agentic data profiling pipeline with MCP server."""

__version__ = "1.0.0"

from file_profiler.main import (
    run,
    profile_file,
    profile_directory,
    analyze_relationships,
)
from file_profiler.models.file_profile import FileProfile, ColumnProfile
from file_profiler.models.relationships import RelationshipReport
