from pathlib import Path
import sys
from datetime import date

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

project = "TFM: Análisis de Tendencias Económicas en Madrid mediante Modelos Financieros y de Sentimiento"
author = "Julia Escudero, Marco Mendieta, Marco Pacora, Piero Rios, Juan Carlos Romero"
copyright = f"{date.today().year}, {author}"
version = "0.1"
release = "0.1.0"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "myst_parser",
    "sphinx.ext.viewcode",
]
autosummary_generate = True
autodoc_default_options = {"members": True, "undoc-members": True, "show-inheritance": True}
autodoc_mock_imports = ["snowflake", "snowflake.connector", "yfinance", "sqlalchemy", "pandas"]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
language = "es"
source_suffix = {".rst": "restructuredtext", ".md": "markdown"}

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
html_title = project
add_module_names = False

def setup(app):
    app.add_css_file('custom.css')