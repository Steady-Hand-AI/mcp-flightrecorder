"""Build the installed resource from the single canonical schema."""
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py


class BuildWithSchema(build_py):
    def run(self):
        super().run()
        destination = Path(self.build_lib) / "mcp_flightrecorder" / "schema.sql"
        self.copy_file("sql/schema.sql", str(destination))


setup(cmdclass={"build_py": BuildWithSchema})
