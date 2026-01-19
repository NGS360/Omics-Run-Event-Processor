NAME=ngs360-omics-run-event-processor

create-lambda-package:
	rm -rf lambda-package
	mkdir lambda-package && \
	cd lambda-package && \
	cp ../lambda.py . && \
	pip3 install -r ../requirements.txt -t . && \
	zip -r ../lambda-package.zip .
	aws s3 cp lambda-package.zip s3://${DATA_LAKE_BUCKET}/${BUCKET_PREFIX}/lambda-package.zip --sse

cf-create: create-lambda-package
	
	aws cloudformation create-stack --stack-name $(NAME) --template-body file://$(NAME).yaml --capabilities CAPABILITY_IAM --parameters file://parameters.json

cf-update:
	aws cloudformation update-stack --stack-name $(NAME) --template-body file://$(NAME).yaml --capabilities CAPABILITY_IAM --parameters file://parameters.json
