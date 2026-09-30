#!/bin/sh
set -e

awslocal sns create-topic --name order-events
awslocal sqs create-queue --queue-name payment-queue
awslocal sqs create-queue --queue-name inventory-queue

SNS_ARN=$(awslocal sns get-topic-attributes \
  --topic-arn arn:aws:sns:us-east-1:000000000000:order-events \
  --query 'Attributes.TopicArn' --output text)

PAYMENT_URL=$(awslocal sqs get-queue-url --queue-name payment-queue --query QueueUrl --output text)
INVENTORY_URL=$(awslocal sqs get-queue-url --queue-name inventory-queue --query QueueUrl --output text)

PAYMENT_ARN=$(awslocal sqs get-queue-attributes --queue-url "$PAYMENT_URL" \
  --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

INVENTORY_ARN=$(awslocal sqs get-queue-attributes --queue-url "$INVENTORY_URL" \
  --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

awslocal sns subscribe --topic-arn "$SNS_ARN" --protocol sqs --notification-endpoint "$PAYMENT_ARN"
awslocal sns subscribe --topic-arn "$SNS_ARN" --protocol sqs --notification-endpoint "$INVENTORY_ARN"

echo "Local AWS messaging resources initialized."
