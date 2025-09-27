
.\deploy\build.ps1

ssh flower "mkdir -p ~/companies-catalogue"

scp -r bin root@flower:~/companies-catalogue/
ssh flower "chmod -R 755 ~/companies-catalogue/bin"

scp pipeline.py root@flower:~/companies-catalogue/
scp requirements.txt root@flower:~/companies-catalogue/

# setup python environment with deps. run dagster
ssh flower "cd companies-catalogue && python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt && dagster job execute -f pipeline.py -j companies_catalogue_job"
# dagster-daemon run --python-file pipeline.py