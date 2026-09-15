"""Repo-wide configuration shared by every use case.

Mirrors ../../java-sdk's ``-Ddemo.namespace`` JVM system property, but as an
environment variable (Python has no per-JVM-process system-property
equivalent): set ``DEMO_NAMESPACE`` before running to point the whole
cookbook at a different namespace. Defaults to ``test``, same as the other
two modules.
"""

import os

NAMESPACE = os.environ.get("DEMO_NAMESPACE", "test")
