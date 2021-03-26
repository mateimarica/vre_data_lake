import setuptools


with open("README.md") as fp:
    long_description = fp.read()

aws_sdk_version = '1.94.1'

setuptools.setup(
    name="vre_data_lake",
    version="0.0.1",
    description="Data Lake Example",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Sandy Chapman, Lixar IT Inc.",
    package_dir={"": "vre_data_lake"},
    packages=setuptools.find_packages(where="vre_data_lake"),
    install_requires=[
        f"aws-cdk.core=={aws_sdk_version}",
        f"aws-cdk.aws-iam=={aws_sdk_version}",
        f"aws-cdk.aws-s3=={aws_sdk_version}",
        f"aws-cdk.aws-s3-deployment=={aws_sdk_version}",
        f"aws-cdk.aws-glue=={aws_sdk_version}",
        f"aws-cdk.aws-lakeformation=={aws_sdk_version}",
        f"aws-cdk.aws-athena=={aws_sdk_version}",
    ],
    python_requires=">=3.9",
)
