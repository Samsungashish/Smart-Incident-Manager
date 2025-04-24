import boto3
import json
import os
import uuid
from datetime import datetime
import re
bedrock_agent_runtime = boto3.client("bedrock-agent-runtime")
s3 = boto3.client("s3")
S3_BUCKET ="hackathon-smart-incident-manager-final"
# Replace this with your actual KB ID
KNOWLEDGE_BASE_ID = 'C57ZM7GPHM'  # Or hard-code it
def lambda_handler(event, context):
    # Read question from event
    input_key = event["Records"][0]["s3"]["object"]["key"]
    if not input_key:
        return {"statusCode": 400, "body": "Missing 's3_key' in event"}
		
		
	# Step 1: Read input JSON from S3
    obj = s3.get_object(Bucket=S3_BUCKET, Key=input_key)
    input_data = json.loads(obj['Body'].read())
    question = input_data.get("question")
    job_name = input_data.get("job_name")
    queryId = input_data.get("queryId")
    title = input_data.get("title")
    if not question:
        return {"statusCode": 400, "body": "No 'question' found in the input JSON"}
    																  
    response = bedrock_agent_runtime.retrieve_and_generate(
        input={
            "text": question
        },
        retrieveAndGenerateConfiguration={
            "type": "KNOWLEDGE_BASE",
            "knowledgeBaseConfiguration": {
                "knowledgeBaseId": KNOWLEDGE_BASE_ID,
                "modelArn": "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-sonnet-20240229-v1:0"
            }
        }
    )
    answer = response["output"]["text"]
    get_severity = f"what is severity of {job_name} and who is its Team Owner and its email ID. And what is URL of Runbook: {job_name}"
    response2 = bedrock_agent_runtime.retrieve_and_generate(
        input={
            "text": get_severity
        },
        retrieveAndGenerateConfiguration={
            "type": "KNOWLEDGE_BASE",
            "knowledgeBaseConfiguration": {
                "knowledgeBaseId": KNOWLEDGE_BASE_ID,
                "modelArn": "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-sonnet-20240229-v1:0"
            }
        }
    )
    severity_op = response2["output"]["text"]
    # print(severity_op)
    # severity_op = severity_op.lower()
    match = re.search(r'\b(P[1-4])\b', severity_op)
    if match:
        severity = match.group(1)
    else:
        severity = "P4"
    # Extract team name (word(s) before "Team")
    team_match = re.search(r"(\b[\w\s&]+)\s+Team", severity_op)
    # Extract email
    email_match = re.search(r"[\w\.-]+@[\w\.-]+", severity_op)
    # Extract URL
    url_match = re.search(r"https?://[^\s]+", severity_op)
    team_name = str(team_match.group(1).strip().split(" ")[-1]) if team_match else "Not Available"
    email_id = str(email_match.group(0)) if email_match else "None Available"
    runbook_url = str(url_match.group(0)) if url_match else "Not available"
    # print("Severity:", severity)
    # print("Team Name:", team_match)
    # print("Email ID:", email_match)
    # print("RunBook URL:", runbook_url)
    data = {
        "queryId": queryId,				
        "question": question,
        "answer": answer,
        "timestamp": datetime.utcnow().isoformat(),
        "foundAnswer": True if answer and ("abcdxxxxdefj" not in answer) and ("No answer found" not in answer) and ("Sorry," not in answer) and ("unable to assist you" not in answer) else False,
        "title": title,
        "status": "SUCCESS" if answer and ("abcdxxxxdefj" not in answer) and ("No answer found" not in answer) and ("Sorry," not in answer) and ("unable to assist you" not in answer) else "PENDING",
        "severity": str(severity),
        "job_name": job_name,
        "team_name": str(team_name),
        "email_id": str(email_id),
        "runbook_url": str(runbook_url)
    }
    # print("#######")
    # print(data)
    # Write to S3
    file_name = f"output-lamda/{queryId}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=file_name,
        Body=json.dumps(data),
        ContentType="application/json"
    )
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Success",
			"queryId": queryId,
            "s3_path": f"s3://{S3_BUCKET}/{file_name}",
            "data": data
        })
    }
