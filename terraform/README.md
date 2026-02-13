# Terraform resource deployment

To authenticate with your S3 provider (doesn't have to be AWS), set these environment variables before running terraform commands:

```
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=
AWS_ENDPOINT_URL=
```

You must have pre-created a bucket called `terraform-state` which is accessible with those credentials.

The credentials must also have permissions to create new buckets in your account.

## To deploy

In either `./dev` or `./prod`.

```bash
cd terraform/dev
terraform plan -out=tfplan
terraform apply tfplan
```
