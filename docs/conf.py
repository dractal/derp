"""Sphinx configuration for Derp documentation."""

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

# -- Project information ---------------------------------------------------

project = "Derp"
copyright = "2026, Aahil Mehta"
author = "Aahil Mehta"
release = "0.1.0"

# -- General configuration -------------------------------------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx_copybutton",
    "sphinx_design",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- Napoleon (Google-style docstrings) ------------------------------------

napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = False
napoleon_use_admonition_for_examples = True
napoleon_use_rtype = True

# -- Autodoc ---------------------------------------------------------------

autodoc_default_options = {
    "members": True,
    "undoc-members": False,
    "show-inheritance": True,
    "member-order": "bysource",
}
autodoc_typehints = "description"
autodoc_class_signature = "separated"

autodoc_mock_imports = [
    "clerk_backend_api",
    "valkey",
    "glide",
    "celery",
]

# -- Autosummary -----------------------------------------------------------

autosummary_generate = True

# -- Intersphinx -----------------------------------------------------------

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pydantic": ("https://docs.pydantic.dev/latest/", None),
}

# -- HTML output -----------------------------------------------------------

html_theme = "furo"
html_title = "Derp"
html_static_path = ["_static"]

html_theme_options = {
    "navigation_with_keys": True,
    "footer_icons": [],
    "source_repository": "https://github.com/dractal/derp",
    "source_branch": "main",
    "source_directory": "docs/",
}

html_show_sphinx = False

# -- Suppress noisy cross-reference warnings --------------------------------

nitpicky = False
suppress_warnings = ["ref.python"]

# -- Copy button -----------------------------------------------------------

copybutton_prompt_text = r">>> |\.\.\. |\$ "
copybutton_prompt_is_regexp = True
