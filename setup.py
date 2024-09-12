import setuptools

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()


__version__ = "0.0.1"

REPO_NAME = "financial_data_scraping"
AUTHOR_USER_NAME = "rydercodes"
SRC_REPO = "financial_data_scraping"
AUTHOR_EMAIL = "jaber.rahimifard@outlook.com"


setuptools.setup(
    name=REPO_NAME,
    version=__version__,
    author=AUTHOR_USER_NAME,
    author_email=AUTHOR_EMAIL,
    description="Scraping financial data from yahoo",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=f"https://github/{AUTHOR_USER_NAME}/{REPO_NAME}",
    project_urls={
        "Bug Tracker": f"https://github/{AUTHOR_USER_NAME}/{REPO_NAME}/issues",
    },
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src")
)