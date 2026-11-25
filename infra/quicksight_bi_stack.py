"""Stack 3: Amazon QuickSight-as-Code (DataSource, SPICE Semantic DataSet & RLS Binding)."""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk import aws_quicksight as quicksight
from constructs import Construct


class QuickSightBiStack(cdk.Stack):
    """Provisions governed Amazon QuickSight DataSource and SPICE Semantic DataSet."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        redshift_workgroup_name: str,
        redshift_db_name: str = "evm_dw",
        **kwargs: object,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        redshift_host = (
            f"{redshift_workgroup_name}.{self.account}.{self.region}.redshift-serverless.amazonaws.com"
        )

        # 1. Governed Redshift Serverless Connection for QuickSight
        self.qs_data_source = quicksight.CfnDataSource(
            self,
            "QuickSightRedshiftSource",
            aws_account_id=self.account,
            data_source_id="evm-redshift-semantic-source",
            name="EVM Redshift Governed Semantic Warehouse",
            type="REDSHIFT",
            data_source_parameters=quicksight.CfnDataSource.DataSourceParametersProperty(
                redshift_parameters=quicksight.CfnDataSource.RedshiftParametersProperty(
                    database=redshift_db_name,
                    host=redshift_host,
                    port=5439,
                )
            ),
        )

        # 2. SPICE Dataset backed by Governed Semantic View + Calculated EVM Ratio Fields
        self.qs_dataset = quicksight.CfnDataSet(
            self,
            "QuickSightEvmSpiceDataset",
            aws_account_id=self.account,
            data_set_id="evm-executive-semantic-spice-dataset",
            name="Enterprise EVM & Utilization Governed Semantic Dataset",
            import_mode="SPICE",
            physical_table_map={
                "FactEvmSemantic": quicksight.CfnDataSet.PhysicalTableProperty(
                    custom_sql=quicksight.CfnDataSet.CustomSqlProperty(
                        data_source_arn=self.qs_data_source.attr_arn,
                        name="v_semantic_evm_portfolio",
                        sql_query="SELECT * FROM gold.v_semantic_evm_portfolio",
                        columns=[
                            quicksight.CfnDataSet.InputColumnProperty(
                                name="snapshot_date", type="DATETIME"
                            ),
                            quicksight.CfnDataSet.InputColumnProperty(
                                name="project_id", type="STRING"
                            ),
                            quicksight.CfnDataSet.InputColumnProperty(
                                name="project_name", type="STRING"
                            ),
                            quicksight.CfnDataSet.InputColumnProperty(
                                name="region_code", type="STRING"
                            ),
                            quicksight.CfnDataSet.InputColumnProperty(
                                name="practice_area", type="STRING"
                            ),
                            quicksight.CfnDataSet.InputColumnProperty(
                                name="bac_usd", type="DECIMAL"
                            ),
                            quicksight.CfnDataSet.InputColumnProperty(
                                name="cum_pv_usd", type="DECIMAL"
                            ),
                            quicksight.CfnDataSet.InputColumnProperty(
                                name="cum_ev_usd", type="DECIMAL"
                            ),
                            quicksight.CfnDataSet.InputColumnProperty(
                                name="cum_ac_total_usd", type="DECIMAL"
                            ),
                        ],
                    )
                )
            },
            logical_table_map={
                "GovernedEvmMetrics": quicksight.CfnDataSet.LogicalTableProperty(
                    alias="Enterprise EVM Semantic Metrics",
                    source=quicksight.CfnDataSet.LogicalTableSourceProperty(
                        physical_table_id="FactEvmSemantic"
                    ),
                    data_transforms=[
                        quicksight.CfnDataSet.TransformOperationProperty(
                            create_columns_operation=quicksight.CfnDataSet.CreateColumnsOperationProperty(
                                columns=[
                                    quicksight.CfnDataSet.CalculatedColumnProperty(
                                        column_id="calc_cpi",
                                        column_name="Cost Performance Index (CPI)",
                                        expression="sum({cum_ev_usd}) / nullIf(sum({cum_ac_total_usd}), 0)",
                                    ),
                                    quicksight.CfnDataSet.CalculatedColumnProperty(
                                        column_id="calc_spi",
                                        column_name="Schedule Performance Index (SPI)",
                                        expression="sum({cum_ev_usd}) / nullIf(sum({cum_pv_usd}), 0)",
                                    ),
                                    quicksight.CfnDataSet.CalculatedColumnProperty(
                                        column_id="calc_eac",
                                        column_name="Estimate at Completion (EAC)",
                                        expression="sum({bac_usd}) / nullIf(sum({cum_ev_usd}) / nullIf(sum({cum_ac_total_usd}), 0), 0)",
                                    ),
                                ]
                            )
                        )
                    ],
                )
            },
        )

        cdk.CfnOutput(self, "QuickSightDataSourceId", value=self.qs_data_source.data_source_id)
        cdk.CfnOutput(self, "QuickSightDataSetId", value=self.qs_dataset.data_set_id)
