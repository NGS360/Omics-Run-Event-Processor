NAME=ngs360-omics-run-event-processor
FULLNAME=ngs360-omics-run-event-processor-LambdaFunction-L3z8fJzoS4Dx
ZIPFILE=$(NAME).zip
BUCKET_PREFIX?=omics-run-events
# Stack holding the GitHub Actions OIDC role the CICD workflow assumes to deploy.
ROLE_STACK=$(NAME)-github-oidc-role

all: help
.PHONY: all help clean install-deps test create-lambda-package upload-package cf-create cf-update cf-create-role cf-update-role lambda-update

help:
	@echo "Target                Description"
	@echo "------                -----------"
	@echo "clean                 Remove build and test artifacts"
	@echo "install-deps          Install runtime and development dependencies"
	@echo "test                  Run the test suite and write coverage reports"
	@echo "create-lambda-package Build the deployment zip"
	@echo "upload-package        Build and upload the zip to S3 (stack creation only)"
	@echo "cf-create             Create the CloudFormation stack"
	@echo "cf-update             Create a change set against the stack"
	@echo "cf-create-role        Create the GitHub Actions OIDC deploy role stack"
	@echo "cf-update-role        Update the GitHub Actions OIDC deploy role stack"
	@echo "lambda-update         Publish a new version and move the branch alias to it"

clean:
	rm -rf lambda-package/ __pycache__/ tests/__pycache__/ .pytest_cache/ .coverage coverage.xml htmlcov/ $(ZIPFILE)

cf-create: upload-package
	aws cloudformation create-stack --stack-name $(NAME) --template-body file://$(NAME).yaml --capabilities CAPABILITY_IAM --parameters file://parameters.json

install-deps:
	pip3 install -r requirements-dev.txt
	pip3 install -r requirements.txt

test:
	python3 -m pytest -vv --cov ./
	coverage xml
	coverage html

create-lambda-package:
	rm -rf lambda-package $(ZIPFILE)
	mkdir -p lambda-package
	cp *.py lambda-package/
	pip3 install -r requirements.txt -t lambda-package/
	cd lambda-package && \
	curl -L "https://github.com/google/go-containerregistry/releases/latest/download/go-containerregistry_Linux_x86_64.tar.gz" -o crane.tar.gz && \
	tar -xzf crane.tar.gz crane && \
	chmod +x crane && \
	rm crane.tar.gz && \
	zip -r ../$(ZIPFILE) .

# Only the stack-create path needs S3: the template reads Code from the bucket,
# so the package has to be there before the function exists. Routine deploys go
# straight to the function with --zip-file and never touch S3.
upload-package: create-lambda-package
	@test -n "$(DATA_LAKE_BUCKET)" || { echo "DATA_LAKE_BUCKET is not set"; exit 1; }
	aws s3 cp $(ZIPFILE) s3://$(DATA_LAKE_BUCKET)/$(BUCKET_PREFIX)/lambda-package.zip --sse


cf-update:
	aws cloudformation create-change-set --change-set-name updateStack --stack-name $(NAME) --template-body file://$(NAME).yaml --capabilities CAPABILITY_IAM --parameters file://parameters.json

# CAPABILITY_NAMED_IAM rather than the CAPABILITY_IAM above: the role template
# sets an explicit RoleName, which CloudFormation treats as the stronger
# capability. github-oidc-deploy-role.yaml is carried only on the bms-ips
# deployment mirror, so these two targets do nothing useful from a clone of the
# public repository.
cf-create-role:
	aws cloudformation create-stack --stack-name $(ROLE_STACK) --template-body file://github-oidc-deploy-role.yaml --capabilities CAPABILITY_NAMED_IAM

cf-update-role:
	aws cloudformation update-stack --stack-name $(ROLE_STACK) --template-body file://github-oidc-deploy-role.yaml --capabilities CAPABILITY_NAMED_IAM

# Publish a version and move the branch's alias to it. Consumers are pinned to
# these alias names -- see the OMICS_REGISTER_WORKFLOW_LAMBDA settings in
# NGS360-APIServer.yaml -- so the branch and its alias are not spelled the same
# way, unlike NGS360-BatchEventTrigger-lambda where the alias is the branch.
#
# Everything runs in one shell under `set -e` so a failed publish aborts the
# target. Splitting it would let an AccessDenied on update-function-code fall
# through to update-alias with an empty --function-version, burying the real
# error under an argument-validation message.
lambda-update: create-lambda-package
	set -e; \
	BRANCH=$$(git branch --show-current); \
	BRANCH=$${BRANCH:-$$GITHUB_REF_NAME}; \
	case "$$BRANCH" in \
	  main)    ALIAS=prod ;; \
	  staging) ALIAS=staging ;; \
	  develop) ALIAS=dev ;; \
	  *) echo "No alias is mapped to branch '$$BRANCH'"; exit 1 ;; \
	esac; \
	COMMIT_MSG=$$(git log --format=format:%s -1); \
	FNVERSION=$$(aws lambda update-function-code --function-name $(FULLNAME) --zip-file fileb://$(ZIPFILE) --publish --query 'Version' --output text); \
	echo "Published version $$FNVERSION; pointing alias $$ALIAS at it"; \
	aws lambda update-alias --name "$$ALIAS" --function-name $(FULLNAME) --function-version "$$FNVERSION" --description "$$COMMIT_MSG"
