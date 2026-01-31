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
                "DetailType": "cicd-deploy-success",
                "Detail": json.dumps({
                    "message": "Deployment completed, trigger Glue Workflow",
                    "event": event
                }),
                "EventBusName": EVENT_BUS_NAME
            }
        ]
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Custom event sent to EventBridge",
            "response": response
        })
    }
