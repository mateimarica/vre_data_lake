from aws_cdk import (
    core,
    aws_s3 as s3,
    aws_glue as glue,
    aws_iam as iam,
)
from typing import List, Optional

from vre_data_lake.role import Role
from vre_data_lake.zone import TablePermission, Zone
from vre_data_lake.filetype import Filetype


class Dataset(core.Construct):
    
    def __init__(self, scope: core.Construct, id: str,
            description: str,
            filetype: Filetype,
            zone: Zone,
            s3_prefix: str,
            lifecycle_rules: List[s3.LifecycleRule],
            crawler_classifer: Optional[glue.CfnClassifier]=None,
            crawler_schedule: Optional[glue.CfnCrawler.ScheduleProperty]=None,
    ):
        super().__init__(scope, id=id)

        self._s3_prefix = s3_prefix
        self._zone = zone

        zone.register_resource(s3_prefix=s3_prefix)
        self.table = zone.create_table(s3_prefix=s3_prefix, description=description)
        zone.create_crawler(s3_prefix=s3_prefix, filetype=filetype, crawler_classifer=crawler_classifer, crawler_schedule=crawler_schedule)
        zone.add_lifecycle_rules(s3_prefix=s3_prefix, lifecycle_rules=lifecycle_rules)

    def grant_access_to_role(self, role: Role, table_permissions: Optional[List[TablePermission]]=[]):
        lake_permissions = self._zone.grant_table_access_to_role(
            role=role,
            s3_prefix=self._s3_prefix,
            table_permissions=table_permissions
        )
        lake_permissions.node.add_dependency(self.table)
