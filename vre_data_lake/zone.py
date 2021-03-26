from enum import Enum
import json
from typing import List, Optional
from aws_cdk import (
    core,
    aws_s3 as s3,
    aws_iam as iam,
    aws_s3_deployment as s3_deploy,
    aws_glue as glue,
    aws_lakeformation as lf,
)
from vre_data_lake.filetype import Filetype
from vre_data_lake.role import Role
import re

class DatabasePermission(Enum):
    ALTER = 'ALTER'
    CREATE_TABLE = 'CREATE_TABLE'
    DESCRIBE = 'DESCRIBE'
    DROP = 'DROP'
    SUPER = 'Super'

class TablePermission(Enum):
    ALTER = 'ALTER'
    DELETE = 'DELETE'
    DESCRIBE = 'DESCRIBE'
    DROP = 'DROP'
    INSERT = 'INSERT'
    SELECT = 'SELECT'
    SUPER = 'Super'


class Zone(core.Construct):

    def __init__(self, scope: core.Construct, id: str, *,
            zone_name: str,
            location_registration_role: iam.Role,
            sample_data_path: Optional[str]=None,
    ):
        super().__init__(scope, id=id)

        if re.match('[a-zA-Z][0-9a-zA-Z_]+', zone_name) == None:
            raise AttributeError(f'"zone_name" must container only alphanumerical characters and underscores. zone_name given was {zone_name}')

        self.zone_name = zone_name
        self.location_registration_role = location_registration_role

        self._bucket = s3.Bucket(self, f'{id}.s3.bucket',
            bucket_name=zone_name.replace('_', '-'),
            removal_policy=core.RemovalPolicy.DESTROY
        )

        self.location_registration_role.attach_inline_policy(
            iam.Policy(self, f'{id}.registration-policy',
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "s3:ListBucket"
                        ],
                        resources=[self._bucket.bucket_arn]
                    ),
                ]
            )
        )

        if sample_data_path is not None:
            source = s3_deploy.Source.asset(
                path=sample_data_path
            )
            s3_deploy.BucketDeployment(self, f'{id}.s3.sample_data',
                destination_bucket=self._bucket,
                sources=[source]
            )

        self.glue_db = glue.Database(self, f'{id}.glue.db',
            database_name=zone_name, # Name must be alphanumeric + underscore for Athena
        )

        self.crawler_role = Role(self, f'{id}.iam.role.glue',
            role_name=f'{id}-Crawler-Role',
            assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
            inline_policies=[
                iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["s3:*"],
                            resources=[self._bucket.bucket_arn]
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["s3:*"],
                            resources=[self._bucket.arn_for_objects('*')]
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["lakeformation:GetDataAccess"],
                            resources=["*"]
                        )
                    ]
                )
            ],
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AwsGlueServiceRole'),
                iam.ManagedPolicy.from_aws_managed_policy_name('AmazonAthenaFullAccess')
            ]
        )

        # Next, we grant permission for the Glue crawler service role to create tables in the zone's database.
        lf.CfnPermissions(self, f'{id}.lake.permissions.crawler.create_table',
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(data_lake_principal_identifier=self.crawler_role.role_arn),
            resource=lf.CfnPermissions.ResourceProperty(
                database_resource=lf.CfnPermissions.DatabaseResourceProperty(
                    catalog_id=self.glue_db.catalog_id, 
                    name=self.glue_db.database_name
                )
            ),
            permissions=['CREATE_TABLE', 'ALTER', 'DESCRIBE']
        )

    def _authorize_crawling_resource(self, s3_prefix: str):
        resource_arn = self._bucket.arn_for_objects(f'{s3_prefix}/*')
        lf.CfnPermissions(self, f'{self.node.id}.{s3_prefix}lake.permissions.crawler.access_s3',
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(data_lake_principal_identifier=self.crawler_role.role_arn),
            resource=lf.CfnPermissions.ResourceProperty(
                data_location_resource=lf.CfnPermissions.DataLocationResourceProperty(
                    catalog_id=self.glue_db.catalog_id,
                    s3_resource=resource_arn
                )
            ),
            permissions=['DATA_LOCATION_ACCESS']
        )
        self.grant_table_access_to_role(
            role=self.crawler_role,
            s3_prefix=s3_prefix,
            table_permissions=[
                TablePermission.ALTER,
                TablePermission.DESCRIBE,
            ]
        )

    def create_table(self, s3_prefix: str, description: str) -> glue.CfnTable:
        location = self._bucket.s3_url_for_object(s3_prefix)
        if not location.endswith('/'):
            location = f'{location}/'
        return glue.CfnTable(self, f'{self.node.id}.{s3_prefix}.glue.table',
            catalog_id="265456890698",
            database_name=self.glue_db.database_name,
            table_input=glue.CfnTable.TableInputProperty(
                description=description,
                name=s3_prefix,
                owner="owner",
                retention=0,
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=[],
                    location=location,
                    compressed=True,
                    number_of_buckets=-1,
                    serde_info=glue.CfnTable.SerdeInfoProperty(parameters={}),
                    bucket_columns=[],
                    sort_columns=[],
                    parameters={
                        "CrawlerSchemaDeserializerVersion": "1.0",
                        "CrawlerSchemaSerializerVersion": "1.0",
                        "averageRecordSize": "0",
                        "classification": "UNKNOWN",
                        "compressionType": "unknown",
                        "has_encrypted_data": "false",
                        "objectCount": "0",
                        "recordCount": "0",
                        "sizeKey": "0",
                        "typeOfData": "file"
                    },
                    stored_as_sub_directories=False
                ),
                partition_keys=[],
                table_type="EXTERNAL_TABLE",
                parameters={
                    "CrawlerSchemaDeserializerVersion": "1.0",
                    "CrawlerSchemaSerializerVersion": "1.0",
                    "averageRecordSize": "0",
                    "classification": "UNKNOWN",
                    "compressionType": "unknown",
                    "has_encrypted_data": "false",
                    "objectCount": "0",
                    "recordCount": "0",
                    "sizeKey": "0",
                    "typeOfData": "file"
                }
            )
        )

    def create_crawler(
            self, 
            s3_prefix: str, 
            filetype: Filetype, 
            crawler_schedule: Optional[glue.CfnCrawler.ScheduleProperty]=None,
            crawler_classifer: Optional[glue.CfnClassifier]=None, 
    ):
        if crawler_classifer != None:
            classifiers = [crawler_classifer.ref]
        else:
            classifiers = None
        crawler = glue.CfnCrawler(self, f'{self.node.id}.{s3_prefix}.glue.crawler',
            description=f"Crawls the data lake dataset named '{s3_prefix}'.",
            role=self.crawler_role.role_arn,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(path=self._bucket.s3_url_for_object(s3_prefix))
                ]
            ),
            classifiers=classifiers,
            database_name=self.glue_db.database_name,
            name=f'{self.zone_name}-{s3_prefix}-crawler',
            schedule=crawler_schedule,
            configuration=json.dumps(dict(
                Grouping=dict(
                    TableGroupingPolicy="CombineCompatibleSchemas"
                ),
                Version=1.0
            ))
        )
        self._authorize_crawling_resource(s3_prefix=s3_prefix)
        return crawler

    def register_resource(self, s3_prefix):
        # This is the S3 ARN for the dataset.
        resource_arn = self._bucket.arn_for_objects(f'{s3_prefix}/*')

        # We allow the Lake Formation's service role to be allowed to register this location.
        # https://docs.aws.amazon.com/lake-formation/latest/dg/registration-role.html
        self.location_registration_role.attach_inline_policy(
            iam.Policy(self, f'{self.node.id}.{s3_prefix}.registration-policy',
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "s3:PutObject",
                            "s3:GetObject",
                            "s3:DeleteObject"
                        ],
                        resources=[resource_arn]
                    )
                ]
            )
        )

        # We then register that location with the service role.
        lf.CfnResource(self, f'{self.node.id}.{s3_prefix}.lakeformation.resource',
            resource_arn=resource_arn,
            use_service_linked_role=False,
            role_arn=self.location_registration_role.role_arn
        )

    def add_lifecycle_rules(self, s3_prefix: str, lifecycle_rules: List[s3.LifecycleRule]):
        for lifecycle_rule in lifecycle_rules:
            if 'prefix' in lifecycle_rule._values:
                del lifecycle_rule._values['prefix']
            prefix = s3_prefix
            if not prefix.endswith('/'):
                prefix = f'{prefix}/'
            self._bucket.add_lifecycle_rule(prefix=prefix, **lifecycle_rule._values)

    @staticmethod
    def _map_db_permissions_to_iam_permissions(database_permissions: List[DatabasePermission]):
        permission_map = {
            DatabasePermission.ALTER: 'glue:UpdateDatabase',
            DatabasePermission.CREATE_TABLE: 'glue:CreateTable',
            DatabasePermission.DESCRIBE: 'glue:GetDatabase',
            DatabasePermission.DROP: 'glue:DeleteDatabase',
            DatabasePermission.SUPER: 'glue:*Database*',
        }
        return [permission_map[p] for p in database_permissions]

    def grant_db_access_to_role(self, role: Role, database_permissions: List[DatabasePermission]):
        if not database_permissions:
            return
        lf.CfnPermissions(self, f'{self.node.id}.lake.permissions.{role.input_role_name}.db_permissions',
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(data_lake_principal_identifier=role.role_arn),
            resource=lf.CfnPermissions.ResourceProperty(
                database_resource=lf.CfnPermissions.DatabaseResourceProperty(
                    catalog_id=self.glue_db.catalog_id, 
                    name=self.glue_db.database_name
                )
            ),
            permissions=[p.value for p in database_permissions]
        )

        role.attach_inline_policy(
            iam.Policy(self, f'{self.node.id}.{role.input_role_name}.db.permissions',
                policy_name=f'{self.zone_name}-DB-Policy',
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=self._map_db_permissions_to_iam_permissions(database_permissions),
                        resources=[self.glue_db.database_arn]
                    )
                ]
            )
        )

    @staticmethod
    def _map_table_permissions_to_glue_iam_permissions(table_permissions: List[TablePermission]):
        permission_map = {
            TablePermission.ALTER: ['glue:UpdateTable'],
            TablePermission.DELETE: None,
            TablePermission.DESCRIBE: ['glue:GetTable'],
            TablePermission.DROP: ['glue:DeleteTable'],
            TablePermission.INSERT: None,
            TablePermission.SELECT: None,
            TablePermission.SUPER: ['glue:*Table*', 'glue:*Partition*'],
        }
        iam_permissions_list = [
            iam_permission 
            for p in permission_map if permission_map[p] and p in table_permissions 
            for iam_permission in permission_map[p]
        ]
        return iam_permissions_list

    @staticmethod
    def _map_table_permissions_to_lake_iam_permissions(table_permissions: List[TablePermission]):
        permission_map = {
            TablePermission.ALTER: None,
            TablePermission.DELETE: "lakeformation:GetDataAccess",
            TablePermission.DESCRIBE: None,
            TablePermission.DROP: None,
            TablePermission.INSERT: "lakeformation:GetDataAccess",
            TablePermission.SELECT: "lakeformation:GetDataAccess",
            TablePermission.SUPER: "lakeformation:GetDataAccess",
        }
        return [permission_map[p] for p in table_permissions if permission_map[p]]

    @staticmethod
    def _map_table_permissions_to_s3_iam_permissions(table_permissions: List[TablePermission]):
        permission_map = {
            TablePermission.ALTER: None,
            TablePermission.DELETE: ["s3:DeleteObject"],
            TablePermission.DESCRIBE: None,
            TablePermission.DROP: None,
            TablePermission.INSERT: ["s3:PutObject"],
            TablePermission.SELECT: ["s3:GetObject","s3:GetObjectVersion"],    # This is sufficient for data access
            TablePermission.SUPER: ["s3:DeleteObject", "s3:PutObject", "s3:GetObject","s3:GetObjectVersion"],
        }
        s3_permissions_list = [
            s3_permission 
            for p in permission_map if permission_map[p] and p in table_permissions 
            for s3_permission in permission_map[p]
        ]
        return s3_permissions_list

    def grant_table_access_to_role(self, role: Role, s3_prefix: str, table_permissions: List[TablePermission]) -> lf.CfnPermissions:
        if not table_permissions:
            return
        lake_permissions = lf.CfnPermissions(self, f'{self.node.id}.{s3_prefix}.lake.permissions.{role.input_role_name}',
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(data_lake_principal_identifier=role.role_arn),
            resource=lf.CfnPermissions.ResourceProperty(
                table_resource=lf.CfnPermissions.TableResourceProperty(
                    catalog_id=self.glue_db.catalog_id, 
                    database_name=self.glue_db.database_name,
                    name=s3_prefix
                )
            ),
            permissions=[p.value for p in table_permissions]
        )
        s3_actions = self._map_table_permissions_to_s3_iam_permissions(table_permissions)
        glue_actions = self._map_table_permissions_to_glue_iam_permissions(table_permissions)
        lake_actions = self._map_table_permissions_to_lake_iam_permissions(table_permissions)
        statements = []
        if glue_actions:
            statements.append(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=glue_actions,
                    resources=[self.glue_db.database_arn]
                )
            )
        if lake_actions:
            statements.append(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=lake_actions,
                    resources=["*"] # Resource type must be * for lake formation.
                )
            )
        if s3_actions:
            statements.append(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=s3_actions,
                    resources=[self._bucket.arn_for_objects(f"{s3_prefix}/*")]
                )
            )
            statements.append(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["s3:ListBucket"],           # This is sufficient for bucket access
                    resources=[self._bucket.bucket_arn]
                )
            )
        if statements:
            role.attach_inline_policy(
                iam.Policy(self, f'{self.node.id}.{role.input_role_name}.table.{s3_prefix}.permissions',
                    policy_name=f'{self.zone_name}-{s3_prefix}-Table-Policy',
                    statements=statements
                )
            )

        return lake_permissions
