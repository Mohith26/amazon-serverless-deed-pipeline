"""DeedStream — serverless county-records pipeline (Lambda handlers + shared logic).

Runs against LocalStack (emulated AWS) for local development, testing and
measurement. The same handler code deploys unchanged to real AWS via the SAM
template at the repo root.
"""

__all__ = ["config", "data_gen", "fetcher", "parser", "query"]
