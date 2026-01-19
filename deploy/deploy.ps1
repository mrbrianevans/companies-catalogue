
.\deploy\build.ps1

ssh flower "mkdir -p ~/companies-catalogue"

scp -r bin root@flower:~/companies-catalogue/
ssh flower "chmod -R 755 ~/companies-catalogue/bin"

scp pipeline.py root@flower:~/companies-catalogue/
scp requirements.txt root@flower:~/companies-catalogue/
scp workspace.yaml root@flower:~/companies-catalogue/

ssh flower "bun add @duckdb/node-bindings-linux-x64"
# setup python environment with deps. run dagster
#ssh flower "cd companies-catalogue && python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt && nohup dagster-daemon run &"
# dagster job execute -f pipeline.py -j companies_catalogue_job
# dagster schedule start daily_5am_schedule
# nohup bun --watch bin/serveEvents.ts & > serveEvents.log