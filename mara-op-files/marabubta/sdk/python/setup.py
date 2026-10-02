# Marabunta - Licensed under the MIT License.
from setuptools import setup, find_packages

setup(
    name="marabunta-torch",
    version="0.1.0",
    description="Marabunta Distributed Low-Communication (DiLoCo) SDK for PyTorch",
    author="Marabunta Inc.",
    packages=find_packages(),
    install_requires=[
        "torch>=2.0.0",
        "requests>=2.25.0",
    ],
    python_requires=">=3.8",
)