import json
import os
import boto3

eventbridge = boto3.client("events")

EVENT_BUS_NAME = os.environ.get("EVENT_BUS_NAME", "default")

def lambda_handler(event, context):

    response = eventbridge.put_events(
        Entries=[
            {
                "Source": "custom.netflix.cicd",
                "DetailType": "TriggerGlueWorkflow",
                "Detail": json.dumps({
                    "message": "Lambda triggered EventBridge",
                    "status": "success"
                }),
                "EventBusName": EVENT_BUS_NAME
            }
        ]
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Event sent to EventBridge successfully",
            "response": response
        })
    }
