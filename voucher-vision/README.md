# VoucherVision

This MAS tries to transcribe the label from the image and retrieves the specimen information.
We will call the VoucherVision API to do this, available at https://leafmachine.org/vouchervisiongo/.
This MAS will first retrieve the image from the OpenDS, then call the VoucherVision API to transcribe the label and retrieve the specimen information
VoucherVision first retrieves the label and then prompt a LLM to parse the label information.

## Environment Variables
The following environment variables must be set for the service to function correctly:
- `RABBITMQ_USER`  
- `RABBITMQ_PASSWORD`  
- `RABBITMQ_HOST`  
- `RABBITMQ_QUEUE`  
- `RABBITMQ_ROUTING_KEY`  -> defaults to `mas-annotation`
- `RABBITMQ_EXCHANGE`  -> defaults to `mas-annotation-exchange`
- `API_ENDPOINT` -> defaults to `https://vouchervision-go-738307415303.us-central1.run.app/`
- `API_KEY` -> API key for VoucherVision, can be obtained from https://leafmachine.org/vouchervisiongo/ 