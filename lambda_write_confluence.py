import boto3
import json
import os
bedrock_agent_runtime = boto3.client("bedrock-agent-runtime")
# Replace this with your actual KB ID
KNOWLEDGE_BASE_ID = "C57ZM7GPHM" # Or hard-code it
def lambda_handler(event, context):
    # Read question from event
    question = event.get("question") or "What is this knowledge base about?"
    try:
        response = bedrock_agent_runtime.retrieve_and_generate(
            input={
                "text": question
            },
            knowledgeBaseId=KNOWLEDGE_BASE_ID,
            retrieveAndGenerateConfiguration={
                "type": "KNOWLEDGE_BASE",
                "knowledgeBaseConfiguration": {
                    "modelArn":"arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-sonnet-20240229-v1:0",
                    "knowledgeBaseId": KNOWLEDGE_BASE_ID,
                    "retrievalConfiguration": {
                        "vectorSearchConfiguration": {
                            "numberOfResults": 5
                        },
                        "type": "KNOWLEDGE_BASE"
                    },
                    "promptTemplate": {
                        "textPromptTemplate": "Answer the following using the provided context:\n\n{{context}}\n\nQuestion: {{query}}\n\nAnswer:"
                    }
                }
            }
        )
        answer = response["output"]["text"]
        return {
            "statusCode": 200,
            "body": json.dumps({
                "question": question,
                "answer": answer
            })
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
