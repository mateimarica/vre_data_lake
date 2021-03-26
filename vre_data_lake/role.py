from typing import Optional
from aws_cdk import (
    core,
    aws_iam as iam,
    aws_athena as athena,
    aws_s3 as s3,
)

class Role(iam.Role):

    def __init__(self, scope: core.Construct, id: str, create_athena_scratch_bucket: Optional[bool]=False, **kwargs):
        super().__init__(scope, id, **kwargs)
        self.input_role_name = kwargs['role_name']
        if create_athena_scratch_bucket:
            self._create_athena_workgroup()

    def _create_athena_workgroup(self):
        athena_output_bucket = s3.Bucket(self, f'{self.node.id}.s3.athena-output',
            bucket_name=f'{self.input_role_name}.athena-output'.lower(),
            removal_policy=core.RemovalPolicy.DESTROY
        )

        workgroup = athena.CfnWorkGroup(self, f'{self.node.id}.athena.workgroup',
            name=f"{self.input_role_name}-workgroup",
            description="Athena Workgroup for the VRE Data Lake.",
            recursive_delete_option=True,
            state='ENABLED',
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=False,
                publish_cloud_watch_metrics_enabled=True,
                requester_pays_enabled=False,
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f'{athena_output_bucket.s3_url_for_object()}/'
                ),
                engine_version=athena.CfnWorkGroup.EngineVersionProperty(
                    selected_engine_version="Athena engine version 2",
                    effective_engine_version="Athena engine version 2"
                )
            )
        )

        self.attach_inline_policy(
            iam.Policy(self, f'{self.node.id}.iam.athena-bucket-access',
                policy_name='Athena-S3-Access',
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "s3:GetBucketLocation",
                            "s3:ListAllMyBuckets"
                        ],
                        resources=["*"]
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=["s3:ListBucket"],
                        resources=[athena_output_bucket.bucket_arn]
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=["s3:*"],
                        resources=[athena_output_bucket.arn_for_objects("*")]
                    )
                ]
            )
        )
