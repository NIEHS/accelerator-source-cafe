from setuptools import setup, find_packages

setup(
    name="accelerator-source-cafe",
    version="0.1.0",
    description="accelerator source cafe ingest",
    author="Deep Patel",
    author_email="deep.patel@nih.gov",
    url="https://github.com/yourusername/accelerator-source-cafe",
    packages=find_packages(),
    install_requires=[open("requirements.txt").read()],
    license="BSD 3-Clause",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
)
