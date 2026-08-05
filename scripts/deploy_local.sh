#!/usr/bin/env bash
# One-command deploy of the DeedStream SAM stack to LocalStack (emulated AWS).
# Requires: LocalStack running on :4566, and samlocal (aws-sam-cli-local).
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> samlocal build"
samlocal build

echo "==> samlocal deploy (LocalStack)"
samlocal deploy \
  --stack-name deedstream \
  --resolve-s3 \
  --no-confirm-changeset \
  --no-fail-on-empty-changeset \
  --capabilities CAPABILITY_IAM \
  --tags Project=deedstream

echo "==> stack outputs"
awslocal cloudformation describe-stacks --stack-name deedstream \
  --query 'Stacks[0].Outputs' --output table
