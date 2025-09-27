doctl compute droplet create `
    --image "ubuntu-25-04-x64" `
    --size "s-1vcpu-1gb-amd" `
    --region lon1 `
    --vpc-uuid "c45e35a6-d1ce-48ac-a3fd-90b95d48e6de" `
    --enable-monitoring `
    --tag-names 'companies-catalogue' `
    --ssh-keys "31:bc:7d:b6:b8:b8:31:bd:91:b9:78:e0:91:56:c6:aa" `
    --wait `
    flower

doctl compute droplet list --format "ID,Name,PublicIPv4"