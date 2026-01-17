# Serve historical events

This directory creates a web server to serve historical events, using the S3 storage bucket populated by eventCapture.

This allows external systems to catch up on historical events further back than Companies House allows.

The web server exposes the same API as the official Companies House one, albeit without any authentication.

Top level paths for `/filings`, `/companies` etc with a mandatory timepoint parameter.

