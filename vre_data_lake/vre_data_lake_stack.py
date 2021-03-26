from vre_data_lake.filetype import Filetype
from vre_data_lake.role import Role
from vre_data_lake.dataset import Dataset, TablePermission
from vre_data_lake.zone import DatabasePermission, Zone
from aws_cdk import (
	core,
	aws_s3 as s3,
	aws_glue as glue,
	aws_iam as iam,
)
from functools import cached_property

class LixarDataLakeStack(core.Stack):

	def __init__(self, scope: core.Construct, id: str, *, 
			data_lake_name: str
	) -> None:
		super().__init__(scope, id=id)

		###############################################################################
		# DEFINE IAM ROLES FOR DATA LAKE
		###############################################################################

		'''
		lake_formation_service_role = iam.Role(self, f'{id}.iam.role.lake-service',
			role_name=f'{id}-lake-formation-service-role',
			assumed_by=iam.CompositePrincipal(
				iam.ServicePrincipal('glue.amazonaws.com'),
				iam.ServicePrincipal('lakeformation.amazonaws.com')
			)
		)

		emr_ec2_role = self._create_emr_roles()

		data_engineer_role = Role(self, f'{id}.iam.role.data-eng',
			create_athena_scratch_bucket=True,
			assumed_by=iam.AccountPrincipal(account_id='265456890698'),
			role_name=f'{id}-DataEngineer'
		)
		data_engineer_role.add_managed_policy(policy=self._athena_access_policy)

		'''
		###############################################################################
		# DEFINE DATA LAKE ZONES AND GRANT PERMISSIONS
		###############################################################################
		'''
		raw_zone = Zone(self, f'{id}.zone.raw', 
			zone_name=f"{data_lake_name}_raw",
			location_registration_role=lake_formation_service_role,
			sample_data_path='./sample_data/raw/'
		)

		raw_zone.grant_db_access_to_role(
			role=data_engineer_role,
			database_permissions=[
				DatabasePermission.DESCRIBE,
			]
		)
		raw_zone.grant_db_access_to_role(
			role=emr_ec2_role,
			database_permissions=[
				DatabasePermission.DESCRIBE
			]
		)

		structured_zone = Zone(self, f'{id}.zone.structured', 
			zone_name=f"{data_lake_name}_structured", # Must be alphanumeric and underscores only for Athena
			location_registration_role=lake_formation_service_role,
			sample_data_path='./sample_data/structured/'
		)

		structured_zone.grant_db_access_to_role(
			role=data_engineer_role,
			database_permissions=[
				DatabasePermission.DESCRIBE,
			]
		)
		structured_zone.grant_db_access_to_role(
			role=emr_ec2_role,
			database_permissions=[
				DatabasePermission.DESCRIBE
			]
		)
		'''

		# curated_zone = Zone(self, f'{id}.zone.raw', 
		#	 zone_name=f"{data_lake_name}_curated"
		# )
		consume_zone = Zone(self, f'{id}.zone.raw', 
			zone_name=f"{data_lake_name}_consume"
		)
		# analytics_zone = Zone(self, f'{id}.zone.raw', 
		#	 zone_name=f"{data_lake_name}_analytics"
		# )

		# At this time, CloudFormation does not support granted Database creation
		# permissions. These permissions must be added manually to the emr task role
		# after deployment via the AWS Console or CLI:
		# E.g. 
		# aws lakeformation grant-permissions \
		#   --principal "DataLakePrincipalIdentifier=arn:aws:iam::265456890698:role/lixar-data-lake-emr-ec2-task-role" \
		#   --resource '{"Catalog":{}}' \
		#   --permissions "CREATE_DATABASE"

		###############################################################################
		# EXAMPLE CSV DATA
		###############################################################################
		'''
		example_data = Dataset(self, f'{id}.dataset.example',
			description="Example dataset illustrating a table CSV format.",
			filetype=Filetype.CSV,
			zone=raw_zone,
			s3_prefix='example_data',
			lifecycle_rules=[
				s3.LifecycleRule(
					enabled=True,
					transitions=[
						s3.Transition(
							storage_class=s3.StorageClass.INTELLIGENT_TIERING,
							transition_after=core.Duration.days(0)
						)
					]
				)
			],
			crawler_schedule=glue.CfnCrawler.ScheduleProperty(
				schedule_expression="cron(0 0 * * ? *)" # Everyday at midnight UTC
			)
		)
		example_data.grant_access_to_role(
			role=data_engineer_role,
			table_permissions=[TablePermission.DESCRIBE, TablePermission.SELECT]
		)
		example_data.grant_access_to_role(
			role=emr_ec2_role,
			table_permissions=[TablePermission.DESCRIBE, TablePermission.SELECT]
		)

		###############################################################################
		# EXAMPLE EXCEL DATA
		###############################################################################
		example_excel = Dataset(self, f'{id}.dataset.example_excel',
			description="Example dataset illustrating a filetype that classifies as UNKNOWN in Glue",
			filetype=Filetype.OTHER,
			zone=raw_zone,
			s3_prefix='example_excel',
			lifecycle_rules=[
				s3.LifecycleRule(
					enabled=True,
					transitions=[
						s3.Transition(
							storage_class=s3.StorageClass.INTELLIGENT_TIERING,
							transition_after=core.Duration.days(0)
						)
					]
				)
			],
			crawler_schedule=glue.CfnCrawler.ScheduleProperty(
				schedule_expression="cron(0 0 * * ? *)" # Everyday at midnight UTC
			)
		)
		example_excel.grant_access_to_role(
			role=data_engineer_role,
			table_permissions=[TablePermission.DESCRIBE, TablePermission.SELECT]
		)
		example_excel.grant_access_to_role(
			role=emr_ec2_role,
			table_permissions=[TablePermission.DESCRIBE, TablePermission.SELECT]
		)

		###############################################################################
		# EXAMPLE SHAPEFILE DATA (Raw zone in ESRI Shapefile)
		###############################################################################
		example_shapefiles_raw = Dataset(self, f'{id}.dataset.example_shapefiles',
			description="Example dataset illustrating a geo-spatial format incompatible with Athena (SHP).",
			filetype=Filetype.OTHER,
			zone=raw_zone,
			s3_prefix='example_shapefiles',
			lifecycle_rules=[
				s3.LifecycleRule(
					enabled=True,
					transitions=[
						s3.Transition(
							storage_class=s3.StorageClass.INTELLIGENT_TIERING,
							transition_after=core.Duration.days(0)
						)
					]
				)
			]
		)
		example_shapefiles_raw.grant_access_to_role(
			role=data_engineer_role,
			table_permissions=[TablePermission.DESCRIBE, TablePermission.SELECT]
		)
		example_shapefiles_raw.grant_access_to_role(
			role=emr_ec2_role,
			table_permissions=[TablePermission.DESCRIBE, TablePermission.SELECT]
		)


		###############################################################################
		# EXAMPLE SHAPEFILE DATA (Structured zone in WKT)
		###############################################################################
		example_shapefiles_wkt = Dataset(self, f'{id}.dataset.example_shapefiles_wkt',
			description="Example dataset illustrating a geo-spatial format compatible with Athena (WKT).",
			filetype=Filetype.CSV,
			zone=structured_zone,
			s3_prefix='example_shapefiles',
			lifecycle_rules=[
				s3.LifecycleRule(
					enabled=True,
					transitions=[
						s3.Transition(
							storage_class=s3.StorageClass.INTELLIGENT_TIERING,
							transition_after=core.Duration.days(0)
						)
					]
				)
			],
			crawler_classifer=self._tsv_classifier
		)
		example_shapefiles_wkt.grant_access_to_role(
			role=data_engineer_role,
			table_permissions=[TablePermission.DESCRIBE, TablePermission.SELECT]
		)
		example_shapefiles_wkt.grant_access_to_role(
			role=emr_ec2_role,
			table_permissions=[TablePermission.DESCRIBE, TablePermission.SELECT, TablePermission.INSERT]
		)
		'''
	@cached_property
	def _athena_access_policy(self) -> iam.ManagedPolicy:
		return iam.ManagedPolicy(self, f'{self.node.id}.iam.policy.athena-access',
			description='Allows access to Athena for querying data.',
			managed_policy_name=f'{self.node.id}-Athena-Access',
			statements=[
				iam.PolicyStatement(
					effect=iam.Effect.ALLOW,
					actions=[
						"athena:CreateNamedQuery",
						"athena:DeleteNamedQuery",
						"athena:GetNamedQuery",
						"athena:GetQueryExecution",
						"athena:GetQueryResults",
						"athena:GetQueryResultsStream",
						"athena:GetWorkGroup",
						"athena:ListNamedQueries",
						"athena:ListQueryExecutions",
						"athena:ListTagsForResource",
						"athena:StartQueryExecution",
						"athena:StopQueryExecution",
						"athena:TagResource",
						"athena:UntagResource"
					],
					resources=[
						f"arn:{self.partition}:athena:{self.region}:{self.account}:workgroup/*" # TODO: Consider locking this down from * workgroups
					]
				),
				iam.PolicyStatement(
					effect=iam.Effect.ALLOW,
					actions=[
						"athena:GetDataCatalog",
						"athena:GetDatabase",
						"athena:GetTableMetadata",
						"athena:ListDataCatalogs",
						"athena:ListDatabases",
						"athena:ListTableMetadata",
						"athena:ListTagsForResource",
						"athena:TagResource",
						"athena:UntagResource"
					],
					resources=[
						f"arn:{self.partition}:athena:{self.region}:{self.account}:datacatalog/*" # TODO: Consider locking this down from * catalogs
					]
				),
				iam.PolicyStatement(
					effect=iam.Effect.ALLOW,
					actions=[
						"athena:ListEngineVersions",
						"athena:ListWorkGroups"
					],
					resources=["*"]
				),
				iam.PolicyStatement(
					effect=iam.Effect.ALLOW,
					actions=[
						"glue:GetDatabase",
						"glue:GetDatabases",
						"glue:GetTable",
						"glue:GetTables",
						"glue:GetPartition",
						"glue:GetPartitions",
						"glue:BatchGetPartition"
					],
					resources=["*"]
				)
			]
		)
	
	def _create_emr_roles(self):
		emr_service_role = Role(self, f'{self.node.id}.iam.role.emr-service',
			role_name=f'{self.node.id}-emr-service-role',
			assumed_by=iam.ServicePrincipal('elasticmapreduce.amazonaws.com')
		)

		emr_service_role.add_managed_policy(
			iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AmazonElasticMapReduceRole')
		)

		emr_ec2_instance_role = Role(self, f'{self.node.id}.iam.role.emr-task',
			role_name=f'{self.node.id}-emr-ec2-task-role',
			assumed_by=iam.ServicePrincipal('ec2.amazonaws.com')
		)

		emr_ec2_instance_role.add_managed_policy(
			iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AmazonElasticMapReduceforEC2Role')
		)

		emr_ec2_instance_role.attach_inline_policy(
			policy=iam.Policy(self, f'{self.node.id}.cloudwatch-logs-emr',
				statements=[
					iam.PolicyStatement(
						actions=[
							"logs:CreateLogGroup",
							"logs:CreateLogStream",
							"logs:PutLogEvents",
							"logs:DescribeLogStreams"
						],
						effect=iam.Effect.ALLOW,
						resources=["*"]
					)
				]
			)
		)
		iam.CfnInstanceProfile(self, f'{self.node.id}.iam.instance-profile.emr-task',
			roles=[emr_ec2_instance_role.role_name],
			instance_profile_name=emr_ec2_instance_role.role_name
		)

		core.CfnOutput(self, f'{self.node.id}',
			value=emr_ec2_instance_role.role_arn,
			export_name=f'{self.node.id}-emr-task-role-arn',
			description='The ARN of the IAM Role to use for the instance profile in EMR clusters.'
		)

		return emr_ec2_instance_role

	@cached_property
	def _tsv_classifier(self) -> glue.CfnClassifier:
		tsv_classifier = glue.CfnClassifier(self, f'{id}.glue.classifier.tsv',
			csv_classifier=glue.CfnClassifier.CsvClassifierProperty(
				allow_single_column=False,
				contains_header='ABSENT',
				delimiter='\t',
				disable_value_trimming=False,
				name=f'{id}.tsv.classifier',
				quote_symbol='"'
			)
		)
		return tsv_classifier
