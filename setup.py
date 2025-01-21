from setuptools import setup, Extension
from Cython.Build import cythonize

# Define the extension module
extensions = [
    Extension(
        name="estimator",
        sources=[
            "dedupe_estimator/estimator.pyx",
            "dedupe_estimator/src/estimator.cpp",
            "dedupe_estimator/src/lz4.cpp",
        ],
        language="c++",
        extra_compile_args=["-O3"],
    )
]

# Setup function
setup(name="dedupe_estimator", ext_modules=cythonize(extensions, language_level="3"))
