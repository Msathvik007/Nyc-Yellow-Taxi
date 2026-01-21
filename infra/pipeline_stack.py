from constructs import Construct
from aws_cdk import (
    Stack,
    Duration,
    aws_iam as iam,
    aws_stepfunctions as sfn,
)

class NycTaxiSfnStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ----------------------------
        # NAMES (match your ASL)
        # ----------------------------
        JOB_A = "raw-validated-using-stepfunctions"
        JOB_B = "validated-master-stepfunction"
        JOB_C = "curated-stepfunction"

        LAMBDA_QUALITY_GATE = "quality_gate_validated"
        LAMBDA_MASTER_FRESHNESS = "master_data_feshness"
        LAMBDA_SNS_STEWARD = "Sns-email-to-steward"

        # 8 crawlers (as in your ASL)
        CRAWLERS = [
            ("curated_taxidata_enrich", "curated-taxidata-enrich-stepfunction"),
            ("curated_need_review", "curated_need_review"),
            ("master_taxi_zone", "master-taxi-zone-stepfunction"),
            ("master_taxi_zone_xref", "master-taxi-zone-xref"),
            ("master_ratecode", "master_ratecode_stepfunction"),
            ("master_vendor", "master_vendor_step function"),
            ("validated_lookup", "validated-taxi-lookup"),
            ("validated_trips", "validated_taxi_trip_using_step_function"),
        ]

        # ----------------------------
        # IAM Role for State Machine
        # ----------------------------
        role = iam.Role(
            self,
            "StateMachineRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
        )

        # Glue permissions (jobs + crawlers)
        role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:StartJobRun",
                    "glue:GetJobRun",
                    "glue:GetJobRuns",
                    "glue:BatchStopJobRun",
                    "glue:StartCrawler",
                    "glue:GetCrawler",
                ],
                resources=["*"],  # tighten later if you want
            )
        )

        # Lambda invoke permissions (by name is still invoke API; easiest is wildcard)
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=["*"],  # tighten later to exact lambda ARNs if you want
            )
        )

        # If your lambdas publish to SNS, Step Functions doesn't need sns:Publish here.
        # If you later add SNS Publish inside SFN, grant sns:Publish.

        # ----------------------------
        # Helpers for Retry/Catch like your ASL
        # ----------------------------
        fail = sfn.Fail(self, "Fail")

        def add_catch_all(state: sfn.State, result_path="$.error"):
            state.add_catch(fail, errors=["States.ALL"], result_path=result_path)
            return state

        def add_glue_retry(state: sfn.TaskStateBase):
            state.add_retry(
                errors=["Glue.AWSGlueException", "States.TaskFailed"],
                interval=Duration.seconds(10),
                max_attempts=2,
                backoff_rate=2,
            )
            return state

        # ----------------------------
        # RunGlueJobA (sync)  (ASL: arn:aws:states:::glue:startJobRun.sync)
        # ----------------------------
        run_job_a = sfn.CustomState(
            self,
            "RunGlueJobA",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": JOB_A,
                    "Arguments": {
                        "--BUCKET.$": "$.BUCKET",
                        "--run_id.$": "$.run_id",
                    },
                },
                "ResultPath": "$.jobA",
            },
        )
        add_glue_retry(run_job_a)
        add_catch_all(run_job_a)

        # ----------------------------
        # QualityGate (lambda invoke)
        # ----------------------------
        quality_gate = sfn.CustomState(
            self,
            "QualityGate",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": LAMBDA_QUALITY_GATE,
                    "Payload": {
                        "BUCKET.$": "$.BUCKET",
                        "run_id.$": "$.run_id",
                        "threshold": 0.7,
                    },
                },
                "ResultPath": "$.qualityGate",
            },
        )
        add_catch_all(quality_gate)

        # ----------------------------
        # QualityDecision (Choice)
        # ----------------------------
        quality_decision = sfn.Choice(self, "QualityDecision")
        # If pass==true → RunGlueJobB else Fail
        # Note: $.qualityGate.Payload.pass is exactly per your ASL
        # We need to reference it with condition.
        # Using JsonPath: "$.qualityGate.Payload.pass"
        pass_condition = sfn.Condition.boolean_equals("$.qualityGate.Payload.pass", True)

        # ----------------------------
        # RunGlueJobB
        # ----------------------------
        run_job_b = sfn.CustomState(
            self,
            "RunGlueJobB",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": JOB_B,
                    "Arguments": {
                        "--BUCKET.$": "$.BUCKET",
                        "--run_id.$": "$.run_id",
                    },
                },
                "ResultPath": "$.jobB",
            },
        )
        add_glue_retry(run_job_b)
        add_catch_all(run_job_b)

        # ----------------------------
        # MasterDataFreshnessGate (lambda invoke)
        # ----------------------------
        master_freshness = sfn.CustomState(
            self,
            "MasterDataFreshnessGate",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": LAMBDA_MASTER_FRESHNESS,
                    "Payload": {
                        "BUCKET.$": "$.BUCKET",
                        "run_id.$": "$.run_id",
                        "thresholds": {
                            "taxi_zone_golden": {"warn_days": 30, "fail_days": 90},
                            "vendor_golden": {"warn_days": 90, "fail_days": 180},
                            "ratecode_golden": {"warn_days": 180, "fail_days": 365},
                        },
                        "sns_topic_arn.$": "$.sns_topic_arn",
                    },
                },
                "ResultPath": "$.masterFreshness",
            },
        )
        add_catch_all(master_freshness)

        # ----------------------------
        # FreshnessDecision (Choice)
        # PASS or WARN → RunGlueJobC, else Fail
        # ----------------------------
        freshness_decision = sfn.Choice(self, "FreshnessDecision")
        cond_pass = sfn.Condition.string_equals("$.masterFreshness.Payload.status", "PASS")
        cond_warn = sfn.Condition.string_equals("$.masterFreshness.Payload.status", "WARN")

        # ----------------------------
        # RunGlueJobC
        # ----------------------------
        run_job_c = sfn.CustomState(
            self,
            "RunGlueJobC",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": JOB_C,
                    "Arguments": {
                        "--BUCKET.$": "$.BUCKET",
                        "--run_id.$": "$.run_id",
                        "--sns_topic_arn.$": "$.sns_topic_arn",
                    },
                },
                "ResultPath": "$.jobC",
            },
        )
        add_glue_retry(run_job_c)
        add_catch_all(run_job_c)

        # ----------------------------
        # SnsEmailToSteward (lambda invoke)
        # ----------------------------
        sns_email_to_steward = sfn.CustomState(
            self,
            "SnsEmailToSteward",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": LAMBDA_SNS_STEWARD,
                    "Payload": {
                        "BUCKET.$": "$.BUCKET",
                        "run_id.$": "$.run_id",
                        "sns_topic_arn.$": "$.sns_topic_arn",
                        "summary_key.$": "States.Format('audit/job_c/{}/summary.json', $.run_id)",
                    },
                },
                "ResultPath": "$.stewardNotify",
            },
        )
        add_catch_all(sns_email_to_steward)

        # ----------------------------
        # Build ONE crawler branch identical pattern
        # startCrawler -> wait 15 -> getCrawler -> choice (READY => Succeed else wait)
        # ----------------------------
        def crawler_branch(branch_id: str, crawler_name: str) -> sfn.StateMachineFragment:
            # Start crawler (aws-sdk integration)
            start = sfn.CustomState(
                self,
                f"StartCrawler_{branch_id}",
                state_json={
                    "Type": "Task",
                    "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
                    "Parameters": {"Name": crawler_name},
                    "Next": f"WaitCrawler_{branch_id}",
                },
            )

            wait = sfn.Wait(
                self,
                f"WaitCrawler_{branch_id}",
                time=sfn.WaitTime.duration(Duration.seconds(15)),
            )

            get = sfn.CustomState(
                self,
                f"GetCrawler_{branch_id}",
                state_json={
                    "Type": "Task",
                    "Resource": "arn:aws:states:::aws-sdk:glue:getCrawler",
                    "Parameters": {"Name": crawler_name},
                    "ResultPath": "$.crawler",
                    "Next": f"CrawlerDone_{branch_id}",
                },
            )

            done_choice = sfn.Choice(self, f"CrawlerDone_{branch_id}")
            succ = sfn.Succeed(self, f"BranchSuccess_{branch_id}")

            # READY => success, RUNNING/STOPPING => wait again, default wait
            done_choice.when(
                sfn.Condition.string_equals("$.crawler.Crawler.State", "READY"),
                succ,
            )
            done_choice.when(
                sfn.Condition.string_equals("$.crawler.Crawler.State", "RUNNING"),
                wait,
            )
            done_choice.when(
                sfn.Condition.string_equals("$.crawler.Crawler.State", "STOPPING"),
                wait,
            )
            done_choice.otherwise(wait)

            # chain: start -> wait -> get -> choice -> (loops)
            chain = sfn.Chain.start(start).next(wait).next(get).next(done_choice)
            return chain

        # ----------------------------
        # Parallel: RunAllCrawlersInParallel
        # ----------------------------
        parallel = sfn.Parallel(self, "RunAllCrawlersInParallel", result_path="$.crawlers")
        add_catch_all(parallel)

        for branch_id, crawler_name in CRAWLERS:
            parallel.branch(crawler_branch(branch_id, crawler_name))

        success = sfn.Succeed(self, "Success")

        # ----------------------------
        # Connect the whole workflow (matching your ASL)
        # ----------------------------
        # RunGlueJobA -> QualityGate -> QualityDecision
        # QualityDecision pass -> RunGlueJobB -> MasterDataFreshnessGate -> FreshnessDecision
        # FreshnessDecision pass/warn -> RunGlueJobC -> SnsEmailToSteward -> Parallel -> Success
        definition = (
            sfn.Chain.start(run_job_a)
            .next(quality_gate)
            .next(quality_decision)
        )

        # Wire quality choice
        quality_decision.when(pass_condition, run_job_b)
        quality_decision.otherwise(fail)

        # After Job B
        run_job_b.next(master_freshness).next(freshness_decision)

        # Wire freshness choice
        freshness_decision.when(cond_pass, run_job_c)
        freshness_decision.when(cond_warn, run_job_c)
        freshness_decision.otherwise(fail)

        # After Job C
        run_job_c.next(sns_email_to_steward).next(parallel).next(success)

        # ----------------------------
        # State machine
        # ----------------------------
        sfn.StateMachine(
            self,
            "NycTaxiPipelineStateMachine",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            role=role,
            timeout=Duration.hours(3),
        )
