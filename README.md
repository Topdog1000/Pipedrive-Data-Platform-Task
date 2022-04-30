# Pipedrive-Data-Platform-Task
## This program downloads datasets from an S3 bucket, create and update deals in Pipedrive account through the API. 
### There's are some simple transformation logics applied: 
### - Filter deleted deals on datasets
### - Multiply data value field by 2 to generate new value
### - Search existing deals
### - Compare existing deal value with new value 
### - Update existing value in Pipe drive account if it is different from the new value
### - Create new deal if the deal does not exist on the Pipedrive account. 
