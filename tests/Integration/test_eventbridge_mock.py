import boto3
from moto import mock_aws

@mock_aws
def test_eventbridge_put_event():
    client = boto3.client("events", region_name="ap-south-1")

    response = client.put_events(
        Entries=[
            {
                "Source": "custom.netflix.cicd",
                "DetailType": "TriggerGlueWorkflow",
                "Detail": '{"status":"success"}'
            }
        ]
    )

    assert response["FailedEntryCount"] == 0
