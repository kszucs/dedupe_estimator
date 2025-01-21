from setuptools import setup, Extension
from Cython.Build import cythonize

# Define the extension module
extensions = [
    Extension(
        name="dedupe_estimator.estimator",
        sources=[
            "dedupe_estimator/estimator.pyx",
            "dedupe_estimator/src/estimator.cpp",
            "dedupe_estimator/src/lz4.cpp",
        ],
        language="c++",
        extra_compile_args=["-O3"],
    )
]

setup(
    name="dedupe_estimator",
    ext_modules=cythonize(extensions, language_level="3"),
    install_requires=[
        "click",
        "faker",
        "humanize",
        "jinja2",
        "numpy",
        "pandas",
        "pillow",
        "plotly",
        "pyarrow",
        "rich",
        "tqdm",
    ],
    entry_points={
        "console_scripts": [
            "de=dedupe_estimator.cli:cli",
        ],
    },
)
