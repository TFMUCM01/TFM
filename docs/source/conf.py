# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
# -- Project information -----------------------------------------------------
project = "TFM: Análisis de Tendencias Económicas en Madrid mediante Modelos Financieros y de Sentimiento"
author = "Julia Escudero, Marco Mendieta, Marco Pacora, Piero Rios, Juan Carlos Romero"

from datetime import date
copyright = f"{date.today().year}, {author}"

release = "0.1.0"
version = "0.1"

# -- General configuration ---------------------------------------------------
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "myst_parser",
    "sphinx.ext.viewcode",
]
autosummary_generate = True

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
language = "es"

# -- Options for HTML output -------------------------------------------------
html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

