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
<<<<<<< HEAD
                "DetailType": "cicd-deploy-success",
                "Detail": json.dumps({
                    "message": "Deployment completed, trigger Glue Workflow",
                    "event": event
=======
                "DetailType": "TriggerGlueWorkflow",
                "Detail": json.dumps({
                    "message": "Lambda triggered EventBridge",
                    "status": "success"
>>>>>>> develop
                }),
                "EventBusName": EVENT_BUS_NAME
            }
        ]
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
<<<<<<< HEAD
            "message": "Custom event sent to EventBridge",
=======
            "message": "Event sent to EventBridge successfully",
>>>>>>> develop
            "response": response
        })
    }
